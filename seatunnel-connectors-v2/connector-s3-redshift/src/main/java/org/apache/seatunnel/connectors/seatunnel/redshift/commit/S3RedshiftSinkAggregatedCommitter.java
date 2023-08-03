/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.redshift.commit;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkAggregatedCommitter;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.util.FileSystemUtils;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConf;
import org.apache.seatunnel.connectors.seatunnel.redshift.exception.S3RedshiftConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.redshift.exception.S3RedshiftConnectorException;
import org.apache.seatunnel.connectors.seatunnel.redshift.resource.CommitterResource;
import org.apache.seatunnel.connectors.seatunnel.redshift.resource.CommitterResourceManager;
import org.apache.seatunnel.connectors.seatunnel.redshift.sink.S3RedshiftSQLGenerator;
import org.apache.seatunnel.connectors.seatunnel.redshift.state.S3RedshiftFileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.redshift.state.S3RedshiftFileCommitInfo;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import com.google.common.base.Stopwatch;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Slf4j
public class S3RedshiftSinkAggregatedCommitter extends FileSinkAggregatedCommitter
        implements SupportMultiTableSinkAggregatedCommitter<CommitterResource> {
    private final S3RedshiftConf conf;
    private S3RedshiftSQLGenerator sqlGenerator;
    private SeaTunnelRowType defaultRowType;
    private volatile boolean appendOnly = true;
    private transient CommitterResource resource;

    public S3RedshiftSinkAggregatedCommitter(
            FileSystemUtils fileSystemUtils, S3RedshiftConf conf, SeaTunnelRowType rowType) {
        super(fileSystemUtils);
        this.conf = conf;
        this.defaultRowType = rowType;
        this.sqlGenerator = new S3RedshiftSQLGenerator(conf, rowType);
    }

    @Override
    public void init() {
        resource = CommitterResource.createSingleTableResource(conf);
    }

    @Override
    public Optional<MultiTableResourceManager<CommitterResource>> initMultiTableResourceManager(
            int tableSize, int queueSize) {
        CommitterResource resource = CommitterResource.createResource(conf);
        return Optional.of(new CommitterResourceManager(resource));
    }

    @Override
    public void setMultiTableResourceManager(
            Optional<MultiTableResourceManager<CommitterResource>> multiTableResourceManager,
            int queueIndex) {
        if (resource != null) {
            resource.closeSingleTableResource();
        }
        this.resource = multiTableResourceManager.get().getSharedResource().get();
    }

    @Override
    public List<FileAggregatedCommitInfo> restoreCommit(
            List<FileAggregatedCommitInfo> aggregatedCommitInfo) {
        if (aggregatedCommitInfo == null || aggregatedCommitInfo.isEmpty()) {
            log.info("Skip to restore empty commit info");
            this.appendOnly = true;
            return Collections.emptyList();
        }

        try {
            log.info("Start to restore commit info");
            this.appendOnly = conf.isAppendOnlyMode();
            return commit(aggregatedCommitInfo);
        } finally {
            log.info("Finish to restore commit info");
        }
    }

    @Override
    public List<FileAggregatedCommitInfo> commit(
            List<FileAggregatedCommitInfo> aggregatedCommitInfos) {
        if (!aggregatedCommitInfos.isEmpty()) {
            SeaTunnelRowType rowType =
                    aggregatedCommitInfos.stream()
                            .map(f -> ((S3RedshiftFileAggregatedCommitInfo) f).getRowType())
                            .filter(Objects::nonNull)
                            .findFirst()
                            .orElse(defaultRowType);
            defaultRowType = rowType;
            this.sqlGenerator = new S3RedshiftSQLGenerator(conf, rowType);
        }
        return commitS3FilesToRedshiftTable(aggregatedCommitInfos);
    }

    @Override
    public void abort(List<FileAggregatedCommitInfo> aggregatedCommitInfos) {
        if (aggregatedCommitInfos == null || aggregatedCommitInfos.isEmpty()) {
            return;
        }
        aggregatedCommitInfos.forEach(
                aggregatedCommitInfo -> {
                    try {
                        for (Map.Entry<String, LinkedHashMap<String, String>> entry :
                                aggregatedCommitInfo.getTransactionMap().entrySet()) {
                            // delete the transaction dir
                            fileSystemUtils.deleteFile(entry.getKey());
                            log.info("delete transaction directory {} on abort", entry.getKey());
                        }
                    } catch (Exception e) {
                        log.error("abort aggregatedCommitInfo error ", e);
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (resource != null) {
            resource.closeSingleTableResource();
        }
    }

    @Override
    public FileAggregatedCommitInfo combine(List<FileCommitInfo> commitInfos) {
        if (commitInfos == null || commitInfos.size() == 0) {
            return null;
        }

        sortByAppendOnly(commitInfos);

        LinkedHashMap<String, LinkedHashMap<String, String>> aggregateCommitInfo =
                new LinkedHashMap<>();
        LinkedHashMap<String, List<String>> partitionDirAndValuesMap = new LinkedHashMap<>();
        SeaTunnelRowType rowType = null;
        boolean appendOnly = true;
        for (FileCommitInfo commitInfo : commitInfos) {
            if (commitInfo.getPartitionDirAndValuesMap() != null) {
                LinkedHashMap<String, String> needMoveFileMap =
                        aggregateCommitInfo.computeIfAbsent(
                                commitInfo.getTransactionDir(), k -> new LinkedHashMap<>());
                needMoveFileMap.putAll(commitInfo.getNeedMoveFiles());
                if (commitInfo.getPartitionDirAndValuesMap() != null
                        && !commitInfo.getPartitionDirAndValuesMap().isEmpty()) {
                    partitionDirAndValuesMap.putAll(commitInfo.getPartitionDirAndValuesMap());
                }
            }

            S3RedshiftFileCommitInfo s3RedshiftCommitInfo = (S3RedshiftFileCommitInfo) commitInfo;
            if (rowType == null && s3RedshiftCommitInfo.getRowType() != null) {
                rowType = s3RedshiftCommitInfo.getRowType();
            }
            if (!s3RedshiftCommitInfo.isAppendOnly() && appendOnly) {
                appendOnly = false;
            }
        }

        return new S3RedshiftFileAggregatedCommitInfo(
                aggregateCommitInfo, partitionDirAndValuesMap, rowType, appendOnly);
    }

    private synchronized List<FileAggregatedCommitInfo> commitS3FilesToRedshiftTable(
            List<FileAggregatedCommitInfo> commitInfos) {
        if (conf.notAppendOnlyMode() && appendOnly) {
            appendOnly =
                    commitInfos.stream()
                            .map(f -> ((S3RedshiftFileAggregatedCommitInfo) f).isAppendOnly())
                            .allMatch(isAppendOnly -> isAppendOnly);
            if (!appendOnly) {
                log.info("Append-only phase end, change to merge phase");
            }
        }
        boolean appendOnlyCommit = conf.isAppendOnlyMode() || appendOnly;

        List<FileAggregatedCommitInfo> errorCommitInfos = new ArrayList<>();
        for (FileAggregatedCommitInfo commitInfo : commitInfos) {
            try {
                commitS3FilesToRedshiftTable(commitInfo, appendOnlyCommit);
            } catch (Exception e) {
                log.error("commit aggregatedCommitInfo error ", e);
                errorCommitInfos.add(commitInfo);
                throw new S3RedshiftConnectorException(
                        S3RedshiftConnectorErrorCode.AGGREGATE_COMMIT_ERROR, e);
            }
        }
        return errorCommitInfos;
    }

    private void commitS3FilesToRedshiftTable(
            FileAggregatedCommitInfo commitInfo, boolean appendOnlyCommit) throws Exception {
        LinkedHashMap<String, LinkedHashMap<String, String>> transactionGroup =
                commitInfo.getTransactionMap();

        Map<String, Future<Throwable>> taskFutures = new HashMap<>();
        CountDownLatch taskAwaits = new CountDownLatch(transactionGroup.size());
        for (Map.Entry<String, LinkedHashMap<String, String>> transaction :
                transactionGroup.entrySet()) {
            Future<Throwable> future =
                    resource.getCommitWorker()
                            .submit(
                                    () -> {
                                        try {
                                            commitS3FilesToRedshiftTable(
                                                    transaction.getKey(),
                                                    transaction.getValue(),
                                                    appendOnlyCommit);
                                            return null;
                                        } catch (Throwable e) {
                                            log.error(
                                                    "commit file error: {}",
                                                    transaction.getKey(),
                                                    e);
                                            return e;
                                        } finally {
                                            taskAwaits.countDown();
                                        }
                                    });
            taskFutures.put(transaction.getKey(), future);
        }
        taskAwaits.await();

        taskFutures.entrySet().stream()
                .forEach(
                        committer -> {
                            try {
                                Throwable ex = committer.getValue().get();
                                if (ex != null) {
                                    throw new RuntimeException(
                                            "commit transaction dir error: " + committer.getKey(),
                                            ex);
                                }
                            } catch (Exception e) {
                                String message =
                                        String.format(
                                                "commit transaction dir error: %s",
                                                committer.getKey());
                                throw new RuntimeException(message, e);
                            }
                        });
    }

    private void commitS3FilesToRedshiftTable(
            String transactionDir, LinkedHashMap<String, String> files, boolean appendOnlyCommit)
            throws Exception {
        for (Map.Entry<String, String> mvFileEntry : files.entrySet()) {
            String tempFilePath = mvFileEntry.getKey();
            String filepath = null;

            if (!fileSystemUtils.fileExist(tempFilePath)) {
                log.warn("skip not exist file {}", tempFilePath);
            } else if (appendOnlyCommit) {
                filepath = tempFilePath;
                copyS3FileToRedshiftTable(filepath);
            } else if (conf.isCopyS3FileToTemporaryTableMode()) {
                filepath = tempFilePath;
                mergeS3FileToRedshiftWithTemporaryTable(filepath);
            } else {
                filepath = mvFileEntry.getKey();
                mergeS3FileToRedshiftWithExternalTable(tempFilePath, filepath);
            }

            fileSystemUtils.deleteFile(filepath);
            log.info("delete file {} ", filepath);
        }

        // second delete transaction directory
        fileSystemUtils.deleteFile(transactionDir);
        log.info("delete transaction directory {} on merge commit", transactionDir);
    }

    private void copyS3FileToRedshiftTable(String filepath) throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String copySql =
                formatCopyS3FileSql(sqlGenerator.generateCopyS3FileToTargetTableSql(), filepath);
        resource.getRedshiftJdbcClient().execute(copySql);
        log.info(
                "Append-only mode, load s3 file sql: {}, cost: {}ms",
                copySql,
                stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
    }

    private void mergeS3FileToRedshiftWithTemporaryTable(String filepath) throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String truncateTemporaryTableSql = sqlGenerator.generateCleanTemporaryTableSql();
        resource.getRedshiftJdbcClient().execute(truncateTemporaryTableSql);
        log.info(
                "Copy mode, truncate temporary table sql: {}, cost: {}ms",
                truncateTemporaryTableSql,
                stopwatch.elapsed(TimeUnit.MILLISECONDS));

        stopwatch.reset().start();
        String copySql =
                formatCopyS3FileSql(sqlGenerator.generateCopyS3FileToTemporaryTableSql(), filepath);
        resource.getRedshiftJdbcClient().execute(copySql);
        log.info(
                "Copy mode, load temporary table sql: {}, cost: {}ms",
                copySql,
                stopwatch.elapsed(TimeUnit.MILLISECONDS));

        stopwatch.reset().start();
        ImmutablePair<String[], String> sortKeyValueQuerySql =
                sqlGenerator.generateSortKeyValueQuerySql();
        Map<String, ImmutablePair<Object, Object>> realSortValues =
                resource.getRedshiftJdbcClient()
                        .querySortValues(sortKeyValueQuerySql.right, sortKeyValueQuerySql.left);
        log.info(
                "Copy mode, get min max value from tmp table sql: {}, sort key range: {}, cost: {}ms",
                sortKeyValueQuerySql.right,
                realSortValues,
                stopwatch.elapsed(TimeUnit.MILLISECONDS));

        stopwatch.reset().start();
        String mergeTemporaryTableSql = sqlGenerator.generateMergeSql(realSortValues);
        resource.getRedshiftJdbcClient().execute(mergeTemporaryTableSql);
        log.info(
                "Copy mode, merge temporary table to target table sql: {}, cost: {}ms",
                mergeTemporaryTableSql,
                stopwatch.elapsed(TimeUnit.MILLISECONDS));

        stopwatch.reset().start();
        String analyseSql = sqlGenerator.generateAnalyseSql(sortKeyValueQuerySql.left);
        resource.getRedshiftJdbcClient().execute(analyseSql);
        log.info(
                "Copy mode, analyse table sql: {}, sort key range: {}, cost: {}ms",
                analyseSql,
                realSortValues,
                stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private void mergeS3FileToRedshiftWithExternalTable(String tempFilePath, String filepath)
            throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String dropExternalTableSql = sqlGenerator.generateDropExternalTableSql();
        resource.getRedshiftJdbcClient().execute(dropExternalTableSql);
        log.info(
                "External table mode, drop external table sql: {}, cost: {}ms",
                dropExternalTableSql,
                stopwatch.elapsed(TimeUnit.MILLISECONDS));

        stopwatch.reset().start();
        String createExternalTableSql =
                formatCreateExternalTableSql(
                        sqlGenerator.generateCreateExternalTableSql(), filepath);
        resource.getRedshiftJdbcClient().execute(createExternalTableSql);
        log.info(
                "External table mode, create external table sql: {}, cost: {}ms",
                createExternalTableSql,
                stopwatch.elapsed(TimeUnit.MILLISECONDS));

        stopwatch.reset().start();
        fileSystemUtils.renameFile(tempFilePath, filepath, true);
        log.info(
                "External table mode, rename temporary file {} to {}, cost: {}ms",
                tempFilePath,
                filepath,
                stopwatch.elapsed(TimeUnit.MILLISECONDS));

        stopwatch.reset().start();
        ImmutablePair<String[], String> sortKeyValueQuerySql =
                sqlGenerator.generateSortKeyValueQuerySql();
        Map<String, ImmutablePair<Object, Object>> realSortValues =
                resource.getRedshiftJdbcClient()
                        .querySortValues(sortKeyValueQuerySql.right, sortKeyValueQuerySql.left);
        log.info(
                "External table mode, get min max value from external table sql: {}, sort key range: {}, cost: {}ms",
                sortKeyValueQuerySql.right,
                realSortValues,
                stopwatch.elapsed(TimeUnit.MILLISECONDS));

        stopwatch.reset().start();
        String mergeExternalTableSql = sqlGenerator.generateMergeSql(realSortValues);
        resource.getRedshiftJdbcClient().execute(mergeExternalTableSql);
        log.info(
                "External table mode, merge external table to target table sql: {}, cost: {}ms",
                mergeExternalTableSql,
                stopwatch.elapsed(TimeUnit.MILLISECONDS));

        stopwatch.reset().start();
        String analyseSql = sqlGenerator.generateAnalyseSql(sortKeyValueQuerySql.left);
        resource.getRedshiftJdbcClient().execute(analyseSql);
        log.info(
                "External table mode, analyse table sql: {}, sort key range: {}, cost: {}ms",
                analyseSql,
                realSortValues,
                stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private String formatCopyS3FileSql(String sql, String filepath) {
        filepath = filepath.replace("//", "/");
        if (filepath.startsWith("/")) {
            filepath = filepath.substring(1);
        }
        return StringUtils.replace(sql, "${path}", filepath);
    }

    public String formatCreateExternalTableSql(String sql, String filepath) {
        String dir = filepath.replace("//", "/").substring(0, filepath.lastIndexOf("/"));
        if (dir.startsWith("/")) {
            dir = dir.substring(1);
        }
        return StringUtils.replace(sql, "${dir}", dir);
    }

    private static void sortByAppendOnly(List<FileCommitInfo> commitInfos) {
        // sort result: appendOnly first, then non-appendOnly
        Collections.sort(
                commitInfos,
                (a, b) -> {
                    S3RedshiftFileCommitInfo x = (S3RedshiftFileCommitInfo) a;
                    S3RedshiftFileCommitInfo y = (S3RedshiftFileCommitInfo) b;
                    if (x.isAppendOnly() && !y.isAppendOnly()) {
                        return -1;
                    }
                    if (!x.isAppendOnly() && y.isAppendOnly()) {
                        return 1;
                    }
                    return 0;
                });
    }
}
