//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.seatunnel.connectors.seatunnel.redshift.sink;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.event.handler.DataTypeChangeEventDispatcher;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.sink.BaseFileSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.AbstractWriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.WriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.redshift.RedshiftJdbcClient;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConf;
import org.apache.seatunnel.connectors.seatunnel.redshift.datatype.ToRedshiftTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.redshift.exception.S3RedshiftConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.redshift.exception.S3RedshiftConnectorException;
import org.apache.seatunnel.connectors.seatunnel.redshift.state.S3RedshiftFileCommitInfo;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class S3RedshiftChangelogWriter extends BaseFileSinkWriter
        implements SupportMultiTableSinkWriter<RedshiftJdbcClient> {
    private final int flushBufferSize;
    private final int flushBufferInterval;
    private final S3RedshiftConf s3RedshiftConf;
    private final Function<SeaTunnelRow, SeaTunnelRow> keyExtractor;
    private final Map<SeaTunnelRow, SeaTunnelRow> memoryTable;
    private final S3RedshiftChangelogMode changelogMode;
    private final ScheduledExecutorService executorService;
    private volatile boolean schemaChanged = false;
    private final DataTypeChangeEventDispatcher dataTypeChangeEventDispatcher =
            new DataTypeChangeEventDispatcher();
    private final S3RedshiftChangelogWriteStrategy changeStreamlogStrategy;
    private transient RedshiftJdbcClient redshiftJdbcClient;

    @Override
    public Optional<MultiTableResourceManager<RedshiftJdbcClient>> initMultiTableResourceManager(
            int tableSize, int queueSize) {
        return Optional.of(
                new S3RedshiftJdbcMultiTableResourceManager(
                        new RedshiftJdbcClient(
                                s3RedshiftConf.getJdbcUrl(),
                                s3RedshiftConf.getJdbcUser(),
                                s3RedshiftConf.getJdbcPassword(),
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

    public S3RedshiftChangelogWriter(
            WriteStrategy writeStrategy,
            HadoopConf hadoopConf,
            Context context,
            String jobId,
            List<FileSinkState> fileSinkStates,
            SeaTunnelRowType seaTunnelRowType,
            S3RedshiftConf s3RedshiftConf) {
        super(writeStrategy, hadoopConf, context, jobId, fileSinkStates);
        this.s3RedshiftConf = s3RedshiftConf;
        this.changelogMode = s3RedshiftConf.getChangelogMode();
        this.changeStreamlogStrategy = createChangelogStrategy(writeStrategy);
        this.keyExtractor =
                createKeyExtractor(
                        seaTunnelRowType,
                        s3RedshiftConf.getRedshiftTablePrimaryKeys().toArray(new String[0]));
        this.memoryTable = new LinkedHashMap<>();
        this.flushBufferSize = s3RedshiftConf.getChangelogBufferFlushSize();
        this.flushBufferInterval = s3RedshiftConf.getChangelogBufferFlushInterval();
        if (flushBufferInterval > 0 && flushBufferInterval < 1000) {
            throw new IllegalArgumentException(
                    "Flush buffer interval must be greater than 1000ms, but is "
                            + flushBufferInterval);
        }
        if (flushBufferInterval > 0) {
            executorService = Executors.newSingleThreadScheduledExecutor();
            executorService.scheduleWithFixedDelay(
                    () -> {
                        try {
                            flushMemoryTable();
                        } catch (IOException e) {
                            log.error("Schedule flush memory table failed", e);
                        }
                    },
                    flushBufferInterval,
                    flushBufferInterval,
                    TimeUnit.MILLISECONDS);
        } else {
            executorService = null;
        }
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        log.info("received schema change event: " + event);
        schemaChanged = true;
        dataTypeChangeEventDispatcher.reset(writeStrategy.getSeaTunnelRowTypeInfo());
        SeaTunnelRowType newRowType = dataTypeChangeEventDispatcher.handle(event);
        writeStrategy.setSeaTunnelRowTypeInfo(newRowType);
        try {
            updateRedshiftTableSchema(event);
        } catch (Exception e) {
            throw new S3RedshiftConnectorException(
                    S3RedshiftConnectorErrorCode.UPDATE_REDSHIFT_SCHEMA_FAILED,
                    "update redshift table schema failed",
                    e);
        }
        log.info("after change schema :" + newRowType);
    }

    private void updateRedshiftTableSchema(SchemaChangeEvent event) throws Exception {
        if (S3RedshiftChangelogMode.APPEND_ONLY.equals(changelogMode)) {
            // ignore
        } else {
            try (Connection connection = redshiftJdbcClient.getConnection()) {
                if (s3RedshiftConf.isCopyS3FileToTemporaryTableMode()) {
                    List<String> sqlList =
                            getSQLFromSchemaChangeEvent(s3RedshiftConf.getRedshiftTable(), event);
                    String temporaryTable = s3RedshiftConf.getTemporaryTableName();
                    sqlList.addAll(getSQLFromSchemaChangeEvent(temporaryTable, event));
                    for (String sql : sqlList) {
                        connection.createStatement().execute(sql);
                    }
                } else {
                    List<String> sqlList =
                            getSQLFromSchemaChangeEvent(s3RedshiftConf.getRedshiftTable(), event);
                    for (String sql : sqlList) {
                        connection.createStatement().execute(sql);
                    }
                }
            } catch (Exception e) {
                log.error("s3-redshift write error", e);
                throw new IllegalArgumentException(e);
            }
        }
    }

    private List<String> getSQLFromSchemaChangeEvent(String tableName, SchemaChangeEvent event) {
        List<String> sqlList = new ArrayList<>();
        if (event instanceof AlterTableColumnsEvent) {
            ((AlterTableColumnsEvent) event)
                    .getEvents()
                    .forEach(
                            column -> {
                                if (column instanceof AlterTableChangeColumnEvent) {
                                    String sql =
                                            String.format(
                                                    "alter table %s rename column %s to %s",
                                                    tableName,
                                                    ((AlterTableChangeColumnEvent) column)
                                                            .getOldColumn(),
                                                    ((AlterTableChangeColumnEvent) column)
                                                            .getColumn()
                                                            .getName());
                                    sqlList.add(sql);
                                } else if (column instanceof AlterTableModifyColumnEvent) {
                                    throw new UnsupportedOperationException(
                                            "Unsupported modify column event: " + event);
                                } else if (column instanceof AlterTableAddColumnEvent) {
                                    String sql =
                                            String.format(
                                                    "alter table %s add column %s %s default null",
                                                    tableName,
                                                    ((AlterTableAddColumnEvent) column)
                                                            .getColumn()
                                                            .getName(),
                                                    ToRedshiftTypeConverter.INSTANCE.convert(
                                                            ((AlterTableAddColumnEvent) column)
                                                                    .getColumn()));
                                    sqlList.add(sql);
                                } else if (column instanceof AlterTableDropColumnEvent) {
                                    String sql =
                                            String.format(
                                                    "alter table %s drop column %s",
                                                    tableName,
                                                    ((AlterTableDropColumnEvent) column)
                                                            .getColumn());
                                    sqlList.add(sql);
                                } else {
                                    throw new UnsupportedOperationException(
                                            "Unsupported event: " + event);
                                }
                            });
        }
        return sqlList;
    }

    @Override
    public synchronized void write(SeaTunnelRow element) throws IOException {
        switch (element.getRowKind()) {
            case INSERT:
                memoryTable.put(keyExtractor.apply(element), element);
                break;
            case UPDATE_AFTER:
                if (S3RedshiftChangelogMode.APPEND_ON_DUPLICATE_UPDATE.equals(changelogMode)) {
                    memoryTable.put(keyExtractor.apply(element), element);
                } else {
                    log.warn(
                            "ignore row-kind:{} for changelog-mode: {}",
                            element.getRowKind(),
                            changelogMode);
                }
                break;
            case DELETE:
                if (S3RedshiftChangelogMode.APPEND_ON_DUPLICATE_DELETE.equals(changelogMode)) {
                    memoryTable.put(keyExtractor.apply(element), element);
                } else {
                    log.warn(
                            "ignore row-kind: {} for changelog-mode: {}",
                            element.getRowKind(),
                            changelogMode);
                }
                break;
            case UPDATE_BEFORE:
            default:
                log.debug("ignore row:{} for changelog-mode: {}", element, changelogMode);
                break;
        }
        if (memoryTable.size() >= flushBufferSize) {
            flushMemoryTable();
        }
    }

    @Override
    public Optional<FileCommitInfo> prepareCommit() throws IOException {
        flushMemoryTable();
        Optional<FileCommitInfo> commitInfo = super.prepareCommit();
        Optional<FileCommitInfo> result =
                commitInfo
                        .<Optional<FileCommitInfo>>map(
                                fileCommitInfo ->
                                        Optional.of(
                                                new S3RedshiftFileCommitInfo(
                                                        fileCommitInfo.getNeedMoveFiles(),
                                                        fileCommitInfo
                                                                .getPartitionDirAndValuesMap(),
                                                        fileCommitInfo.getTransactionDir(),
                                                        writeStrategy.getSeaTunnelRowTypeInfo())))
                        .orElseGet(
                                () ->
                                        Optional.of(
                                                new S3RedshiftFileCommitInfo(
                                                        null,
                                                        null,
                                                        null,
                                                        writeStrategy.getSeaTunnelRowTypeInfo())));
        schemaChanged = false;
        return result;
    }

    @Override
    public void close() throws IOException {
        if (executorService != null) {
            executorService.shutdownNow();
            try {
                flushMemoryTable();
            } catch (Exception e) {
                log.error("Close flush memory table failed", e);
            }
        }
        super.close();
    }

    private synchronized void flushMemoryTable() throws IOException {
        changeStreamlogStrategy.write(memoryTable.values());
        memoryTable.clear();
    }

    private static Function<SeaTunnelRow, SeaTunnelRow> createKeyExtractor(
            SeaTunnelRowType rowType, String[] keyFields) {
        List<Integer> keyIndex =
                Stream.of(keyFields)
                        .map(field -> rowType.indexOf(field))
                        .collect(Collectors.toList());

        // If there is a data exception, it may be a hashcode conflict
        return row -> {
            Object[] fields = new Object[keyIndex.size()];
            for (int i = 0; i < keyIndex.size(); i++) {
                fields[i] = row.getField(keyIndex.get(i));
            }
            SeaTunnelRow keyRow = new SeaTunnelRow(fields);
            keyRow.setTableId(row.getTableId());
            return keyRow;
        };
    }

    private S3RedshiftChangelogWriteStrategy createChangelogStrategy(WriteStrategy writeStrategy) {
        if (writeStrategy instanceof AbstractWriteStrategy) {
            return new S3RedshiftChangelogWriteStrategy((AbstractWriteStrategy) writeStrategy);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported write strategy: " + writeStrategy.getClass().getName());
        }
    }
}
