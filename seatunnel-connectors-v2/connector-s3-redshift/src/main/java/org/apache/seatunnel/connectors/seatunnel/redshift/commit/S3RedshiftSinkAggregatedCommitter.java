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
import org.apache.commons.lang3.tuple.Pair;

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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class S3RedshiftSinkAggregatedCommitter extends FileSinkAggregatedCommitter
        implements SupportMultiTableSinkAggregatedCommitter<CommitterResource> {
    private final S3RedshiftConf conf;
    private S3RedshiftSQLGenerator sqlGenerator;
    private volatile boolean appendOnly = true;
    private transient CommitterResource resource;

    public S3RedshiftSinkAggregatedCommitter(
            FileSystemUtils fileSystemUtils, S3RedshiftConf conf, SeaTunnelRowType rowType) {
        super(fileSystemUtils);
        this.conf = conf;
        this.sqlGenerator = new S3RedshiftSQLGenerator(conf, rowType);
    }

    @Override
    public MultiTableResourceManager<CommitterResource> initMultiTableResourceManager(
            int tableSize, int queueSize) {
        CommitterResource resource = CommitterResource.createResource(conf);
        return new CommitterResourceManager(resource);
    }

    @Override
    public void setMultiTableResourceManager(
            MultiTableResourceManager<CommitterResource> multiTableResourceManager,
            int queueIndex) {
        this.resource = multiTableResourceManager.getSharedResource().get();
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
            return commitAggregatedCommitInfo(aggregatedCommitInfo, true);
        } finally {
            log.info("Finish to restore commit info");
        }
    }

    @Override
    public List<FileAggregatedCommitInfo> commit(
            List<FileAggregatedCommitInfo> aggregatedCommitInfos) {
        return commitAggregatedCommitInfo(aggregatedCommitInfos, false);
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
        }

        boolean appendOnly =
                commitInfos.stream()
                        .map(S3RedshiftFileCommitInfo.class::cast)
                        .allMatch(S3RedshiftFileCommitInfo::isAppendOnly);

        SeaTunnelRowType rowType = null;
        Set<SeaTunnelRowType> schemaChangedTypes =
                commitInfos.stream()
                        .map(S3RedshiftFileCommitInfo.class::cast)
                        .filter(S3RedshiftFileCommitInfo::isSchemaChanged)
                        .map(S3RedshiftFileCommitInfo::getRowType)
                        .collect(Collectors.toSet());
        if (schemaChangedTypes.size() == 0) {
            if (appendOnly) {
                Set<SeaTunnelRowType> appendOnlyRowTypes =
                        commitInfos.stream()
                                .map(S3RedshiftFileCommitInfo.class::cast)
                                .map(S3RedshiftFileCommitInfo::getRowType)
                                .collect(Collectors.toSet());
                if (appendOnlyRowTypes.size() > 1) {
                    log.error(
                            "There are multiple schema types in checkpoint, commitInfo: {}",
                            commitInfos);
                    throw new RuntimeException("There are multiple schema types in checkpoint");
                }
                rowType = appendOnlyRowTypes.iterator().next();
            } else {
                Set<SeaTunnelRowType> changelogRowTypes =
                        commitInfos.stream()
                                .map(S3RedshiftFileCommitInfo.class::cast)
                                .filter(e -> !e.isAppendOnly())
                                .map(S3RedshiftFileCommitInfo::getRowType)
                                .collect(Collectors.toSet());
                if (changelogRowTypes.size() > 1) {
                    log.error(
                            "There are multiple schema types in checkpoint, commitInfo: {}",
                            commitInfos);
                    throw new RuntimeException("There are multiple schema types in checkpoint");
                }
                rowType = changelogRowTypes.iterator().next();
            }
        } else {
            if (schemaChangedTypes.size() > 1) {
                log.error("Schema changed row type more than one, commitInfo: {}", commitInfos);
                throw new RuntimeException("Schema changed row type more than one");
            }
            rowType = schemaChangedTypes.iterator().next();
        }

        return new S3RedshiftFileAggregatedCommitInfo(
                aggregateCommitInfo, partitionDirAndValuesMap, rowType, appendOnly);
    }

    private List<FileAggregatedCommitInfo> commitAggregatedCommitInfo(
            List<FileAggregatedCommitInfo> aggregatedCommitInfos, boolean restore) {
        if (!aggregatedCommitInfos.isEmpty()) {
            SeaTunnelRowType rowType =
                    aggregatedCommitInfos.stream()
                            .map(f -> ((S3RedshiftFileAggregatedCommitInfo) f).getRowType())
                            .filter(Objects::nonNull)
                            .findFirst()
                            .get();
            this.sqlGenerator = new S3RedshiftSQLGenerator(conf, rowType);
        }
        return commitS3FilesToRedshiftTable(aggregatedCommitInfos, restore);
    }

    private synchronized List<FileAggregatedCommitInfo> commitS3FilesToRedshiftTable(
            List<FileAggregatedCommitInfo> commitInfos, boolean restore) {
        if (conf.notAppendOnlyMode() && appendOnly) {
            appendOnly =
                    commitInfos.stream()
                            .map(f -> ((S3RedshiftFileAggregatedCommitInfo) f).isAppendOnly())
                            .allMatch(isAppendOnly -> isAppendOnly);
            if (!appendOnly) {
                log.info("Append-only mode end, change to merge mode");
            }
        }
        boolean appendOnlyCommit = conf.isAppendOnlyMode() || appendOnly;

        List<FileAggregatedCommitInfo> errorCommitInfos = new ArrayList<>();
        for (FileAggregatedCommitInfo commitInfo : commitInfos) {
            try {
                if (appendOnlyCommit) {
                    parallelCommitS3FilesToRedshiftTable(commitInfo, restore);
                } else {
                    commitS3FilesToRedshiftTable(commitInfo, restore);
                }
            } catch (Exception e) {
                log.error("commit aggregatedCommitInfo error ", e);
                errorCommitInfos.add(commitInfo);
                throw new S3RedshiftConnectorException(
                        S3RedshiftConnectorErrorCode.AGGREGATE_COMMIT_ERROR, e);
            }
        }
        return errorCommitInfos;
    }

    private void commitS3FilesToRedshiftTable(FileAggregatedCommitInfo commitInfo, boolean restore)
            throws Exception {
        LinkedHashMap<String, LinkedHashMap<String, String>> transactionGroup =
                commitInfo.getTransactionMap();
        for (Map.Entry<String, LinkedHashMap<String, String>> transaction :
                transactionGroup.entrySet()) {
            String transactionDir = transaction.getKey();
            if (restore && !fileSystemUtils.fileExist(transactionDir)) {
                log.warn("skip not exist transaction directory {}", transactionDir);
                return;
            }

            LinkedHashMap<String, String> temporaryFiles = transaction.getValue();
            if (!temporaryFiles.isEmpty()) {
                commitS3FilesToRedshiftTable(temporaryFiles, restore);
            }

            // second delete transaction directory
            fileSystemUtils.deleteFile(transactionDir);
            log.info("delete transaction directory {} on merge commit", transaction.getKey());
        }
    }

    private void parallelCommitS3FilesToRedshiftTable(
            FileAggregatedCommitInfo commitInfo, boolean restore) throws Exception {
        LinkedHashMap<String, LinkedHashMap<String, String>> transactionGroup =
                commitInfo.getTransactionMap();

        log.info("Parallel commit transactions {}", transactionGroup.keySet());
        Map<String, Future<Throwable>> taskFutures = new HashMap<>();
        CountDownLatch taskAwaits = new CountDownLatch(transactionGroup.size());
        for (Map.Entry<String, LinkedHashMap<String, String>> transaction :
                transactionGroup.entrySet()) {
            Future<Throwable> future =
                    resource.getCommitWorker()
                            .submit(
                                    () -> {
                                        try {
                                            String transactionDir = transaction.getKey();
                                            if (restore
                                                    && !fileSystemUtils.fileExist(transactionDir)) {
                                                log.warn(
                                                        "skip not exist transaction directory {}",
                                                        transactionDir);
                                                return null;
                                            }

                                            LinkedHashMap<String, String> temporaryFiles =
                                                    transaction.getValue();
                                            if (!temporaryFiles.isEmpty()) {
                                                copyS3FileToRedshiftTable(transactionDir);
                                            }

                                            // second delete transaction directory
                                            fileSystemUtils.deleteFile(transactionDir);
                                            log.info(
                                                    "delete transaction directory {} on merge commit",
                                                    transaction.getKey());
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

    private void commitS3FilesToRedshiftTable(LinkedHashMap<String, String> files, boolean restore)
            throws Exception {
        for (Map.Entry<String, String> mvFileEntry : files.entrySet()) {
            String tempFilePath = mvFileEntry.getKey();

            if (restore && !fileSystemUtils.fileExist(tempFilePath)) {
                log.warn("skip not exist file {}", tempFilePath);
                continue;
            }

            String filepath = tempFilePath;
            mergeS3FileToRedshiftWithTemporaryTable(filepath);

            if (filepath != null) {
                fileSystemUtils.deleteFile(filepath);
            }
            log.info("delete file {} ", filepath);
        }
    }

    private void copyS3FileToRedshiftTable(String filepath) throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String copySql = formatCopyS3FileSql(sqlGenerator.getCopyS3FileToTableSql(), filepath);
        resource.getRedshiftJdbcClient().execute(copySql);
        log.info(
                "Append-only mode, load s3 file sql: {}, cost: {}ms",
                copySql,
                stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
    }

    private void mergeS3FileToRedshiftWithTemporaryTable(String filepath) throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String truncateTemporaryTableSql = sqlGenerator.getCleanTemporaryTableSql();
        resource.getRedshiftJdbcClient().execute(truncateTemporaryTableSql);
        log.info(
                "Merge mode, truncate temporary table sql: {}, cost: {}ms",
                truncateTemporaryTableSql,
                stopwatch.elapsed(TimeUnit.MILLISECONDS));

        stopwatch.reset().start();
        String copySql =
                formatCopyS3FileSql(sqlGenerator.getCopyS3FileToTemporaryTableSql(), filepath);
        resource.getRedshiftJdbcClient().execute(copySql);
        log.info(
                "Merge mode, load temporary table sql: {}, cost: {}ms",
                copySql,
                stopwatch.elapsed(TimeUnit.MILLISECONDS));

        stopwatch.reset().start();
        String queryDeleteRowCountSql = sqlGenerator.getQueryDeleteRowCountSql();
        int deletableRowCount =
                resource.getRedshiftJdbcClient().executeQueryCount(queryDeleteRowCountSql);
        log.info(
                "Merge mode, query delete row sql: {}, cost: {}ms",
                queryDeleteRowCountSql,
                stopwatch.elapsed(TimeUnit.MILLISECONDS));
        if (deletableRowCount > 0) {
            stopwatch.reset().start();
            String deleteRowSql = sqlGenerator.getDeleteTargetTableRowSql();
            int deletedRowCount = resource.getRedshiftJdbcClient().executeUpdate(deleteRowSql);
            if (deletableRowCount != deletedRowCount) {
                log.warn(
                        "Merge mode, deletable row count: {}, deleted row count: {}, delete row sql: {}",
                        deletableRowCount,
                        deletedRowCount,
                        deleteRowSql);
            }
            String cleanDeleteRowCountSql = sqlGenerator.getCleanDeleteRowCountSql();
            resource.getRedshiftJdbcClient().executeUpdate(cleanDeleteRowCountSql);
            log.info(
                    "Merge mode, delete row sql: {}, updates: {}, cost: {}ms",
                    deleteRowSql,
                    deletedRowCount,
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }

        stopwatch.reset().start();
        Pair<String[], String> sortKeyValueQuerySql = sqlGenerator.getSortKeyValueQuerySql();
        Map<String, ImmutablePair<Object, Object>> realSortValues =
                resource.getRedshiftJdbcClient()
                        .querySortValues(
                                sortKeyValueQuerySql.getRight(), sortKeyValueQuerySql.getLeft());
        log.info(
                "Merge mode, get min max value from tmp table sql: {}, sort key range: {}, cost: {}ms",
                sortKeyValueQuerySql.getRight(),
                realSortValues,
                stopwatch.elapsed(TimeUnit.MILLISECONDS));

        stopwatch.reset().start();
        String mergeTemporaryTableSql = sqlGenerator.generateMergeSql(realSortValues);
        resource.getRedshiftJdbcClient().execute(mergeTemporaryTableSql);
        log.info(
                "Merge mode, merge temporary table to target table sql: {}, cost: {}ms",
                mergeTemporaryTableSql,
                stopwatch.elapsed(TimeUnit.MILLISECONDS));

        stopwatch.reset().start();
        String analyseSql = sqlGenerator.generateAnalyseSql(sortKeyValueQuerySql.getLeft());
        resource.getRedshiftJdbcClient().execute(analyseSql);
        log.info(
                "Merge mode, analyse table sql: {}, sort key range: {}, cost: {}ms",
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

        boolean appendOnly = true;
        for (int i = 0; i < commitInfos.size(); i++) {
            S3RedshiftFileCommitInfo s3RedshiftFileCommitInfo =
                    (S3RedshiftFileCommitInfo) commitInfos.get(i);
            if (s3RedshiftFileCommitInfo.isAppendOnly()) {
                if (!appendOnly) {
                    throw new IllegalStateException(
                            "sort result error, appendOnly file after non-appendOnly file");
                }
            } else if (appendOnly && i > 0) {
                log.info(
                        "The checkpoint contains both append and update files,"
                                + " which causes committer to switch to merge mode");
            }
            appendOnly = s3RedshiftFileCommitInfo.isAppendOnly();
        }
    }
}
