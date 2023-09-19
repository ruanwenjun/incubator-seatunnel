package org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.source.reader.fetch;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.debezium.EmbeddedDatabaseHistory;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.config.Oracle9BridgeSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.source.offset.Oracle9BridgeOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.utils.OracleConnectionUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.utils.OracleUtils;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.oracle.Oracle9BridgeConnectorConfig;
import io.debezium.connector.oracle.Oracle9BridgeOffsetContext;
import io.debezium.connector.oracle.OracleChangeEventSourceMetricsFactory;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleErrorHandler;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.OracleTopicSelector;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Collect;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;

@Slf4j
public class Oracle9BridgeSourceFetchTaskContext extends JdbcSourceFetchTaskContext {

    private final OracleConnection connection;
    private Collection<TableChanges.TableChange> engineHistory;
    private final OracleEventMetadataProvider metadataProvider;

    private TopicSelector<TableId> topicSelector;
    private OracleDatabaseSchema databaseSchema;
    private Oracle9BridgeOffsetContext offsetContext;
    private OracleTaskContext taskContext;
    private ChangeEventQueue<DataChangeEvent> queue;
    private JdbcSourceEventDispatcher dispatcher;
    private SnapshotChangeEventSourceMetrics snapshotChangeEventSourceMetrics;

    private OracleErrorHandler errorHandler;

    public Oracle9BridgeSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig,
            JdbcDataSourceDialect dataSourceDialect,
            Collection<TableChanges.TableChange> engineHistory) {
        super(sourceConfig, dataSourceDialect);
        this.connection =
                OracleConnectionUtils.createOracleConnection(sourceConfig.getDbzConfiguration());
        this.engineHistory = engineHistory;
        this.metadataProvider = new OracleEventMetadataProvider();
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        registerDatabaseHistory(sourceSplitBase);

        // initial stateful objects
        final Oracle9BridgeConnectorConfig connectorConfig = getDbzConnectorConfig();
        this.topicSelector = OracleTopicSelector.defaultSelector(connectorConfig);

        this.databaseSchema = OracleUtils.createOracleDatabaseSchema(connectorConfig, connection);
        // todo logMiner or xStream
        this.offsetContext =
                loadStartingOffsetState(
                        new Oracle9BridgeOffsetContext.Loader(connectorConfig), sourceSplitBase);
        validateAndLoadDatabaseHistory(offsetContext, databaseSchema);

        this.taskContext = new OracleTaskContext(connectorConfig, databaseSchema);

        final int queueSize =
                sourceSplitBase.isSnapshotSplit()
                        ? Integer.MAX_VALUE
                        : getSourceConfig().getDbzConnectorConfig().getMaxQueueSize();
        this.queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(connectorConfig.getPollInterval())
                        .maxBatchSize(connectorConfig.getMaxBatchSize())
                        .maxQueueSize(queueSize)
                        .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                        .loggingContextSupplier(
                                () ->
                                        taskContext.configureLoggingContext(
                                                "oracle9bridge-cdc-connector-task"))
                        // do not buffer any element, we use signal event
                        // .buffering()
                        .build();
        this.dispatcher =
                new JdbcSourceEventDispatcher(
                        connectorConfig,
                        topicSelector,
                        databaseSchema,
                        queue,
                        connectorConfig.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        metadataProvider,
                        schemaNameAdjuster);

        final OracleChangeEventSourceMetricsFactory changeEventSourceMetricsFactory =
                new OracleChangeEventSourceMetricsFactory(
                        new OracleStreamingChangeEventSourceMetrics(
                                taskContext, queue, metadataProvider, connectorConfig));

        this.snapshotChangeEventSourceMetrics =
                changeEventSourceMetricsFactory.getSnapshotMetrics(
                        taskContext, queue, metadataProvider);
        this.errorHandler = new OracleErrorHandler(connectorConfig.getLogicalName(), queue);
    }

    @Override
    public void close() {
        try {
            this.connection.close();
        } catch (SQLException e) {
            log.warn("Failed to close connection", e);
        }
    }

    @Override
    public Oracle9BridgeSourceConfig getSourceConfig() {
        return (Oracle9BridgeSourceConfig) sourceConfig;
    }

    public OracleConnection getConnection() {
        return connection;
    }

    @Override
    public Oracle9BridgeConnectorConfig getDbzConnectorConfig() {
        return (Oracle9BridgeConnectorConfig) super.getDbzConnectorConfig();
    }

    @Override
    public Oracle9BridgeOffsetContext getOffsetContext() {
        return offsetContext;
    }

    public SnapshotChangeEventSourceMetrics getSnapshotChangeEventSourceMetrics() {
        return snapshotChangeEventSourceMetrics;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public OracleDatabaseSchema getDatabaseSchema() {
        return databaseSchema;
    }

    @Override
    public SeaTunnelRowType getSplitType(Table table) {
        return OracleUtils.getSplitType(table);
    }

    @Override
    public JdbcSourceEventDispatcher getDispatcher() {
        return dispatcher;
    }

    @Override
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        return getDbzConnectorConfig().getTableFilters().dataCollectionFilter();
    }

    @Override
    public Offset getStreamOffset(SourceRecord sourceRecord) {
        return OracleUtils.getRedoLogPosition(sourceRecord);
    }

    /** Loads the connector's persistent offset (if present) via the given loader. */
    private Oracle9BridgeOffsetContext loadStartingOffsetState(
            Oracle9BridgeOffsetContext.Loader loader, SourceSplitBase oracleSplit) {
        Offset offset =
                oracleSplit.isSnapshotSplit()
                        // todo: fetch the initialize offset from oracle?
                        ? Oracle9BridgeOffset.INITIAL_OFFSET
                        : oracleSplit.asIncrementalSplit().getStartupOffset();

        return loader.load(offset.getOffset());
    }

    private void validateAndLoadDatabaseHistory(
            Oracle9BridgeOffsetContext offset, OracleDatabaseSchema schema) {
        schema.initializeStorage();
        schema.recover(offset);
    }

    private void registerDatabaseHistory(SourceSplitBase sourceSplitBase) {
        EmbeddedDatabaseHistory.registerHistory(
                sourceConfig
                        .getDbzConfiguration()
                        .getString(EmbeddedDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME),
                engineHistory);
    }

    /** Copied from debezium for accessing here. */
    public static class OracleEventMetadataProvider implements EventMetadataProvider {
        @Override
        public Instant getEventTimestamp(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            final Long timestamp = sourceInfo.getInt64(SourceInfo.TIMESTAMP_KEY);
            return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
        }

        @Override
        public Map<String, String> getEventSourcePosition(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            final String scn = sourceInfo.getString(SourceInfo.SCN_KEY);
            return Collect.hashMapOf(SourceInfo.SCN_KEY, scn == null ? "null" : scn);
        }

        @Override
        public String getTransactionId(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            return sourceInfo.getString(SourceInfo.SCN_KEY);
        }
    }
}
