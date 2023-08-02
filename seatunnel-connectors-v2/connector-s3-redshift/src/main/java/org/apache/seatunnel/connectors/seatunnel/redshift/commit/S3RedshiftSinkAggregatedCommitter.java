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
import org.apache.seatunnel.connectors.seatunnel.redshift.RedshiftJdbcClient;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConf;
import org.apache.seatunnel.connectors.seatunnel.redshift.exception.S3RedshiftConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.redshift.exception.S3RedshiftConnectorException;
import org.apache.seatunnel.connectors.seatunnel.redshift.sink.S3RedshiftJdbcMultiTableResourceManager;
import org.apache.seatunnel.connectors.seatunnel.redshift.sink.S3RedshiftSQLGenerator;
import org.apache.seatunnel.connectors.seatunnel.redshift.state.S3RedshiftFileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.redshift.state.S3RedshiftFileCommitInfo;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import com.google.common.base.Stopwatch;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
public class S3RedshiftSinkAggregatedCommitter extends FileSinkAggregatedCommitter
        implements SupportMultiTableSinkAggregatedCommitter<RedshiftJdbcClient> {
    private final S3RedshiftConf conf;
    private S3RedshiftSQLGenerator sqlGenerator;
    private SeaTunnelRowType defaultRowType;
    private transient RedshiftJdbcClient redshiftJdbcClient;

    @Override
    public void init() {
        redshiftJdbcClient =
                new RedshiftJdbcClient(
                        conf.getJdbcUrl(), conf.getJdbcUser(), conf.getJdbcPassword(), 1);
    }

    @Override
    public Optional<MultiTableResourceManager<RedshiftJdbcClient>> initMultiTableResourceManager(
            int tableSize, int queueSize) {
        return Optional.of(
                new S3RedshiftJdbcMultiTableResourceManager(
                        new RedshiftJdbcClient(
                                conf.getJdbcUrl(),
                                conf.getJdbcUser(),
                                conf.getJdbcPassword(),
                                queueSize)));
    }

    @Override
    public void setMultiTableResourceManager(
            Optional<MultiTableResourceManager<RedshiftJdbcClient>> multiTableResourceManager,
            int queueIndex) {
        if (redshiftJdbcClient != null) {
            redshiftJdbcClient.close();
        }
        this.redshiftJdbcClient = multiTableResourceManager.get().getSharedResource().get();
    }

    public S3RedshiftSinkAggregatedCommitter(
            FileSystemUtils fileSystemUtils, S3RedshiftConf conf, SeaTunnelRowType rowType) {
        super(fileSystemUtils);
        this.conf = conf;
        this.defaultRowType = rowType;
        this.sqlGenerator = new S3RedshiftSQLGenerator(conf, rowType);
    }

    @Override
    public List<FileAggregatedCommitInfo> commit(
            List<FileAggregatedCommitInfo> aggregatedCommitInfos) {
        if (conf.isAppendOnlyMode()) {
            return copyS3FilesToRedshiftTable(aggregatedCommitInfos);
        }
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
        return mergeS3FilesToRedshiftTable(aggregatedCommitInfos);
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
        LinkedHashMap<String, LinkedHashMap<String, String>> aggregateCommitInfo =
                new LinkedHashMap<>();
        LinkedHashMap<String, List<String>> partitionDirAndValuesMap = new LinkedHashMap<>();
        commitInfos.stream()
                .filter(f -> f.getPartitionDirAndValuesMap() != null)
                .forEach(
                        commitInfo -> {
                            LinkedHashMap<String, String> needMoveFileMap =
                                    aggregateCommitInfo.computeIfAbsent(
                                            commitInfo.getTransactionDir(),
                                            k -> new LinkedHashMap<>());
                            needMoveFileMap.putAll(commitInfo.getNeedMoveFiles());
                            if (commitInfo.getPartitionDirAndValuesMap() != null
                                    && !commitInfo.getPartitionDirAndValuesMap().isEmpty()) {
                                partitionDirAndValuesMap.putAll(
                                        commitInfo.getPartitionDirAndValuesMap());
                            }
                        });
        Optional<SeaTunnelRowType> rowType =
                commitInfos.stream()
                        .filter(c -> c instanceof S3RedshiftFileCommitInfo)
                        .filter(c -> ((S3RedshiftFileCommitInfo) c).getRowType() != null)
                        .map(c -> ((S3RedshiftFileCommitInfo) c).getRowType())
                        .findFirst();
        return new S3RedshiftFileAggregatedCommitInfo(
                aggregateCommitInfo, partitionDirAndValuesMap, rowType.orElse(null));
    }

    private List<FileAggregatedCommitInfo> copyS3FilesToRedshiftTable(
            List<FileAggregatedCommitInfo> aggregatedCommitInfos) {
        List<FileAggregatedCommitInfo> errorAggregatedCommitInfoList = new ArrayList<>();
        for (FileAggregatedCommitInfo aggregatedCommitInfo : aggregatedCommitInfos) {
            try {
                for (Map.Entry<String, LinkedHashMap<String, String>> entry :
                        aggregatedCommitInfo.getTransactionMap().entrySet()) {
                    for (Map.Entry<String, String> mvFileEntry : entry.getValue().entrySet()) {
                        // first rename temp file
                        fileSystemUtils.renameFile(
                                mvFileEntry.getKey(), mvFileEntry.getValue(), true);
                        log.info(
                                "rename file {} to {} ",
                                mvFileEntry.getKey(),
                                mvFileEntry.getValue());

                        String sql =
                                formatCopyS3FileSql(conf.getExecuteSql(), mvFileEntry.getValue());
                        redshiftJdbcClient.execute(sql);
                        log.info("execute redshift sql is:" + sql);

                        fileSystemUtils.deleteFile(mvFileEntry.getValue());
                        log.info("delete file {} ", mvFileEntry.getValue());
                    }
                    // second delete transaction directory
                    fileSystemUtils.deleteFile(entry.getKey());
                    log.info("delete transaction directory {} on commit", entry.getKey());
                }
            } catch (Exception e) {
                log.error("commit aggregatedCommitInfo error ", e);
                errorAggregatedCommitInfoList.add(aggregatedCommitInfo);
                throw new S3RedshiftConnectorException(
                        S3RedshiftConnectorErrorCode.AGGREGATE_COMMIT_ERROR, e);
            }
        }
        return errorAggregatedCommitInfoList;
    }

    private synchronized List<FileAggregatedCommitInfo> mergeS3FilesToRedshiftTable(
            List<FileAggregatedCommitInfo> aggregatedCommitInfos) {
        List<FileAggregatedCommitInfo> errorAggregatedCommitInfoList = new ArrayList<>();
        for (FileAggregatedCommitInfo aggregatedCommitInfo : aggregatedCommitInfos) {
            try {
                for (Map.Entry<String, LinkedHashMap<String, String>> entry :
                        aggregatedCommitInfo.getTransactionMap().entrySet()) {
                    for (Map.Entry<String, String> mvFileEntry : entry.getValue().entrySet()) {
                        String tempFilePath = mvFileEntry.getKey();
                        String filepath = mvFileEntry.getValue();

                        if (!fileSystemUtils.fileExist(tempFilePath)) {
                            log.warn("skip not exist file {}", tempFilePath);
                        } else if (conf.isCopyS3FileToTemporaryTableMode()) {
                            filepath = tempFilePath;
                            copyS3FileToRedshiftTemporaryTable(filepath);
                        } else {
                            loadS3FileToRedshiftExternalTable(tempFilePath, filepath);
                        }

                        fileSystemUtils.deleteFile(filepath);
                        log.info("delete file {} ", filepath);
                    }
                    // second delete transaction directory
                    fileSystemUtils.deleteFile(entry.getKey());
                    log.info("delete transaction directory {} on merge commit", entry.getKey());
                }
            } catch (Exception e) {
                log.error("commit aggregatedCommitInfo error ", e);
                errorAggregatedCommitInfoList.add(aggregatedCommitInfo);
                throw new S3RedshiftConnectorException(
                        S3RedshiftConnectorErrorCode.AGGREGATE_COMMIT_ERROR, e);
            }
        }
        // TODO errorAggregatedCommitInfoList Always empty, So return is no use
        return errorAggregatedCommitInfoList;
    }

    private void copyS3FileToRedshiftTemporaryTable(String filepath) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        stopwatch.reset().start();
        String truncateTemporaryTableSql = sqlGenerator.generateCleanTemporaryTableSql();
        try {
            redshiftJdbcClient.execute(truncateTemporaryTableSql);
            log.info(
                    "Copy mode, truncate temporary table sql: {}, cost: {}ms",
                    truncateTemporaryTableSql,
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));

            stopwatch.reset().start();
            String copySql =
                    formatCopyS3FileSql(
                            sqlGenerator.generateCopyS3FileToTemporaryTableSql(), filepath);
            redshiftJdbcClient.execute(copySql);
            log.info(
                    "Copy mode, load temporary table sql: {}, cost: {}ms",
                    copySql,
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));

            stopwatch.reset().start();
            ImmutablePair<String[], String> sortKeyValueQuerySql =
                    sqlGenerator.generateSortKeyValueQuerySql();
            Map<String, ImmutablePair<Object, Object>> realSortValues =
                    redshiftJdbcClient.querySortValues(
                            sortKeyValueQuerySql.right, sortKeyValueQuerySql.left);

            log.info(
                    "Copy mode, get min max value from tmp table sql: {}, sort key range: {}, cost: {}ms",
                    sortKeyValueQuerySql.right,
                    realSortValues,
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));

            stopwatch.reset().start();
            String mergeTemporaryTableSql = sqlGenerator.generateMergeSql(realSortValues);
            redshiftJdbcClient.execute(mergeTemporaryTableSql);
            log.info(
                    "Copy mode, merge temporary table to target table sql: {}, sort key range: {}, cost: {}ms",
                    mergeTemporaryTableSql,
                    realSortValues,
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));

            stopwatch.reset().start();
            String analyseSql = sqlGenerator.generateAnalyseSql(sortKeyValueQuerySql.left);
            redshiftJdbcClient.execute(analyseSql);
            log.info(
                    "Copy mode, analyse table sql: {}, sort key range: {}, cost: {}ms",
                    analyseSql,
                    realSortValues,
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            throw new S3RedshiftConnectorException(
                    S3RedshiftConnectorErrorCode.AGGREGATE_COMMIT_ERROR, e);
        }
    }

    private void loadS3FileToRedshiftExternalTable(String tempFilePath, String filepath) {
        try {
            Stopwatch stopwatch = Stopwatch.createStarted();
            String dropExternalTableSql = sqlGenerator.generateDropExternalTableSql();
            redshiftJdbcClient.execute(dropExternalTableSql);
            log.info(
                    "External table mode, drop external table sql: {}, cost: {}ms",
                    dropExternalTableSql,
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));

            stopwatch.reset().start();
            String createExternalTableSql =
                    formatCreateExternalTableSql(
                            sqlGenerator.generateCreateExternalTableSql(), filepath);
            redshiftJdbcClient.execute(createExternalTableSql);
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
                    redshiftJdbcClient.querySortValues(
                            sortKeyValueQuerySql.right, sortKeyValueQuerySql.left);

            log.info(
                    "External table mode, get min max value from external table sql: {}, sort key range: {}, cost: {}ms",
                    sortKeyValueQuerySql.right,
                    realSortValues,
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));

            stopwatch.reset().start();
            String mergeTemporaryTableSql = sqlGenerator.generateMergeSql(realSortValues);
            redshiftJdbcClient.execute(mergeTemporaryTableSql);
            log.info(
                    "External table mode, merge external table to target table sql: {}, sort key range: {}, cost: {}ms",
                    mergeTemporaryTableSql,
                    realSortValues,
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));

            stopwatch.reset().start();
            String analyseSql = sqlGenerator.generateAnalyseSql(sortKeyValueQuerySql.left);
            redshiftJdbcClient.execute(analyseSql);
            log.info(
                    "External table mode, analyse table sql: {}, sort key range: {}, cost: {}ms",
                    analyseSql,
                    realSortValues,
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
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
}
