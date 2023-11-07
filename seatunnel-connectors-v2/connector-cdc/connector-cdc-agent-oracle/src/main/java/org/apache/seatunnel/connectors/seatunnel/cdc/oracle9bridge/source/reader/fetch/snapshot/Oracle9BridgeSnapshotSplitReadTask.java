package org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.source.reader.fetch.snapshot;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.source.offset.Oracle9BridgeOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.utils.Oracle9BridgeClientUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.utils.OracleConnectionUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.utils.OracleUtils;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;

import org.whaleops.whaletunnel.oracleagent.sdk.OracleAgentClient;

import io.debezium.connector.oracle.Oracle9BridgeConnectorConfig;
import io.debezium.connector.oracle.Oracle9BridgeOffsetContext;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.ValueConverter;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import lombok.extern.slf4j.Slf4j;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;

@Slf4j
public class Oracle9BridgeSnapshotSplitReadTask extends AbstractSnapshotChangeEventSource {

    /** Interval for showing a log statement with the progress while scanning a single table. */
    private static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);

    private final Oracle9BridgeConnectorConfig connectorConfig;
    private final OracleDatabaseSchema databaseSchema;
    private final OracleConnection jdbcConnection;
    private final OracleAgentClient oracle9BridgeClient;
    private final JdbcSourceEventDispatcher dispatcher;
    private final Clock clock;
    private final SnapshotSplit snapshotSplit;
    private final Oracle9BridgeOffsetContext offsetContext;
    private final SnapshotProgressListener snapshotProgressListener;

    public Oracle9BridgeSnapshotSplitReadTask(
            Oracle9BridgeConnectorConfig connectorConfig,
            Oracle9BridgeOffsetContext previousOffset,
            SnapshotProgressListener snapshotProgressListener,
            OracleDatabaseSchema databaseSchema,
            OracleConnection jdbcConnection,
            JdbcSourceEventDispatcher dispatcher,
            SnapshotSplit snapshotSplit,
            OracleAgentClient oracle9BridgeClient) {
        super(connectorConfig, snapshotProgressListener);
        this.offsetContext = previousOffset;
        this.connectorConfig = connectorConfig;
        this.databaseSchema = databaseSchema;
        this.jdbcConnection = jdbcConnection;
        this.oracle9BridgeClient = oracle9BridgeClient;
        this.dispatcher = dispatcher;
        this.clock = Clock.system();
        this.snapshotSplit = snapshotSplit;
        this.snapshotProgressListener = snapshotProgressListener;
    }

    @Override
    protected SnapshotResult doExecute(
            ChangeEventSourceContext changeEventSourceContext,
            OffsetContext previousOffset,
            SnapshotContext snapshotContext,
            SnapshottingTask snapshottingTask)
            throws Exception {
        log.info(
                "Begin to execute Oracle9BridgeSnapshotSplitReadTask for split: {}",
                snapshotSplit.splitId());
        final RelationalSnapshotChangeEventSource.RelationalSnapshotContext ctx =
                (RelationalSnapshotChangeEventSource.RelationalSnapshotContext) snapshotContext;
        ctx.offset = offsetContext;

        String table = snapshotSplit.getTableId().table();
        String tableOwner = OracleConnectionUtils.getTableOwner(jdbcConnection, table);
        Integer maxFzsFileNumber =
                Oracle9BridgeClientUtils.currentMaxFzsFileNumber(
                        oracle9BridgeClient, tableOwner, table);
        Long maxScn =
                Oracle9BridgeClientUtils.currentMaxScn(
                        oracle9BridgeClient, tableOwner, table, maxFzsFileNumber);
        // todo: use scn from database as the watermark,
        //  since the scn from oracle9bridge is not accurate, it will <= current scn in database.
        final Oracle9BridgeOffset lowWatermark = new Oracle9BridgeOffset(maxFzsFileNumber, maxScn);
        log.info(
                "Snapshot step 1 - Determining low watermark {} for split {}",
                lowWatermark,
                snapshotSplit);
        ((Oracle9BridgeSnapshotSplitChangeEventSourceContext) changeEventSourceContext)
                .setLowWatermark(lowWatermark);
        dispatcher.dispatchWatermarkEvent(
                offsetContext.getPartition(), snapshotSplit, lowWatermark, WatermarkKind.LOW);

        log.info("Snapshot step 2 - Snapshotting data");
        createDataEvents(ctx, snapshotSplit.getTableId());

        maxFzsFileNumber =
                Oracle9BridgeClientUtils.currentMaxFzsFileNumber(
                        oracle9BridgeClient, tableOwner, table);
        maxScn =
                Oracle9BridgeClientUtils.currentMaxScn(
                        oracle9BridgeClient, tableOwner, table, maxFzsFileNumber);
        final Oracle9BridgeOffset highWatermark = new Oracle9BridgeOffset(maxFzsFileNumber, maxScn);
        log.info(
                "Snapshot step 3 - Determining high watermark {} for split {}",
                highWatermark,
                snapshotSplit);
        ((Oracle9BridgeSnapshotSplitChangeEventSourceContext) changeEventSourceContext)
                .setHighWatermark(highWatermark);
        dispatcher.dispatchWatermarkEvent(
                offsetContext.getPartition(), snapshotSplit, highWatermark, WatermarkKind.HIGH);
        return SnapshotResult.completed(ctx.offset);
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(OffsetContext previousOffset) {
        return new SnapshottingTask(false, true);
    }

    @Override
    protected SnapshotContext prepare(ChangeEventSourceContext changeEventSourceContext)
            throws Exception {
        return new Oracle9BridgeSnapshotContext();
    }

    private void createDataEvents(
            RelationalSnapshotChangeEventSource.RelationalSnapshotContext snapshotContext,
            TableId tableId)
            throws Exception {
        EventDispatcher.SnapshotReceiver snapshotReceiver =
                dispatcher.getSnapshotChangeEventReceiver();
        log.debug("Snapshotting table {}", tableId);
        createDataEventsForTable(
                snapshotContext, snapshotReceiver, databaseSchema.tableFor(tableId));
        snapshotReceiver.completeSnapshot();
    }

    /** Dispatches the data change events for the records of a single table. */
    private void createDataEventsForTable(
            RelationalSnapshotChangeEventSource.RelationalSnapshotContext snapshotContext,
            EventDispatcher.SnapshotReceiver snapshotReceiver,
            Table table)
            throws InterruptedException {

        long exportStart = clock.currentTimeInMillis();
        log.info("Exporting data from split '{}' of table {}", snapshotSplit.splitId(), table.id());

        final String selectSql =
                OracleUtils.buildSplitScanQuery(
                        snapshotSplit.getTableId(),
                        snapshotSplit.getSplitKeyType(),
                        snapshotSplit.getSplitStart() == null,
                        snapshotSplit.getSplitEnd() == null);
        log.info(
                "For split '{}' of table {} using select statement: '{}'",
                snapshotSplit.splitId(),
                table.id(),
                selectSql);

        try (PreparedStatement selectStatement =
                        OracleUtils.readTableSplitDataStatement(
                                jdbcConnection,
                                selectSql,
                                snapshotSplit.getSplitStart() == null,
                                snapshotSplit.getSplitEnd() == null,
                                snapshotSplit.getSplitStart(),
                                snapshotSplit.getSplitEnd(),
                                snapshotSplit.getSplitKeyType().getTotalFields(),
                                connectorConfig.getSnapshotFetchSize());
                ResultSet rs = selectStatement.executeQuery()) {

            ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
            long rows = 0;
            Threads.Timer logTimer = getTableScanLogTimer();

            while (rs.next()) {
                rows++;
                final Object[] row = new Object[columnArray.getGreatestColumnPosition()];
                for (int i = 0; i < columnArray.getColumns().length; i++) {
                    Column actualColumn = table.columns().get(i);
                    row[columnArray.getColumns()[i].position() - 1] =
                            readField(rs, i + 1, actualColumn, table);
                }
                if (logTimer.expired()) {
                    long stop = clock.currentTimeInMillis();
                    log.info(
                            "Exported {} records for split '{}' after {}",
                            rows,
                            snapshotSplit.splitId(),
                            Strings.duration(stop - exportStart));
                    snapshotProgressListener.rowsScanned(table.id(), rows);
                    logTimer = getTableScanLogTimer();
                }
                dispatcher.dispatchSnapshotEvent(
                        table.id(),
                        getChangeRecordEmitter(snapshotContext, table.id(), row),
                        snapshotReceiver);
            }
            log.info(
                    "Finished exporting {} records for split '{}', total duration '{}'",
                    rows,
                    snapshotSplit.splitId(),
                    Strings.duration(clock.currentTimeInMillis() - exportStart));
        } catch (SQLException e) {
            throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
        }
    }

    /**
     * copied from io.debezium.connector.oracle.antlr.listener.ParserUtils#convertValueToSchemaType.
     */
    private Object readField(ResultSet rs, int fieldNo, Column actualColumn, Table actualTable)
            throws SQLException {

        OracleValueConverters oracleValueConverters =
                new OracleValueConverters(connectorConfig, jdbcConnection);

        final SchemaBuilder schemaBuilder = oracleValueConverters.schemaBuilder(actualColumn);
        if (schemaBuilder == null) {
            return null;
        }
        Schema schema = schemaBuilder.build();
        Field field = new Field(actualColumn.name(), 1, schema);
        final ValueConverter valueConverter = oracleValueConverters.converter(actualColumn, field);
        return valueConverter.convert(rs.getObject(fieldNo));
    }

    protected ChangeRecordEmitter getChangeRecordEmitter(
            SnapshotContext snapshotContext, TableId tableId, Object[] row) {
        snapshotContext.offset.event(tableId, clock.currentTime());
        return new SnapshotChangeRecordEmitter(snapshotContext.offset, row, clock);
    }

    private Threads.Timer getTableScanLogTimer() {
        return Threads.timer(clock, LOG_INTERVAL);
    }

    private static class Oracle9BridgeSnapshotContext
            extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext {

        public Oracle9BridgeSnapshotContext() throws SQLException {
            super("");
        }
    }
}
