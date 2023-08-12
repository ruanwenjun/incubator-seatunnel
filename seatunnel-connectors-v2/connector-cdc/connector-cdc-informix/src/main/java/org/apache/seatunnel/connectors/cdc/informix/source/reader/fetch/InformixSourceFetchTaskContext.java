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

package org.apache.seatunnel.connectors.cdc.informix.source.reader.fetch;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.informix.config.InformixSourceConfig;
import org.apache.seatunnel.connectors.cdc.informix.source.offset.InformixOffset;
import org.apache.seatunnel.connectors.cdc.informix.utils.InformixUtils;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.informix.InformixConnection;
import io.debezium.connector.informix.InformixConnectorConfig;
import io.debezium.connector.informix.InformixDatabaseSchema;
import io.debezium.connector.informix.InformixErrorHandler;
import io.debezium.connector.informix.InformixEventMetadataProvider;
import io.debezium.connector.informix.InformixOffsetContext;
import io.debezium.connector.informix.InformixTaskContext;
import io.debezium.connector.informix.InformixTopicSelector;
import io.debezium.connector.informix.InformixValueConverters;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.TopicSelector;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;

@Slf4j
public class InformixSourceFetchTaskContext extends JdbcSourceFetchTaskContext {
    @Getter private final InformixConnection connection;
    private final InformixEventMetadataProvider metadataProvider;
    private TopicSelector<TableId> topicSelector;
    private InformixDatabaseSchema databaseSchema;
    private InformixOffsetContext offsetContext;
    private InformixTaskContext taskContext;
    private ChangeEventQueue<DataChangeEvent> queue;
    private JdbcSourceEventDispatcher dispatcher;
    @Getter private SnapshotChangeEventSourceMetrics snapshotChangeEventSourceMetrics;
    private InformixErrorHandler errorHandler;

    public InformixSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dataSourceDialect) {
        super(sourceConfig, dataSourceDialect);
        this.connection =
                new InformixConnection(sourceConfig.getDbzConnectorConfig().getJdbcConfig());
        this.metadataProvider = new InformixEventMetadataProvider();
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        super.registerDatabaseHistory(sourceSplitBase, connection);
        InformixConnectorConfig connectorConfig = getDbzConnectorConfig();

        this.topicSelector = InformixTopicSelector.defaultSelector(connectorConfig);
        this.databaseSchema =
                new InformixDatabaseSchema(
                        connectorConfig,
                        new InformixValueConverters(),
                        topicSelector,
                        schemaNameAdjuster,
                        false);
        this.offsetContext =
                loadStartingOffsetState(
                        new InformixOffsetContext.Loader(connectorConfig), sourceSplitBase);
        validateAndLoadDatabaseHistory(offsetContext, databaseSchema);

        this.taskContext = new InformixTaskContext(connectorConfig, databaseSchema);

        int queueSize =
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
                                                "informix-cdc-connector-task"))
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

        DefaultChangeEventSourceMetricsFactory changeEventSourceMetricsFactory =
                new DefaultChangeEventSourceMetricsFactory();
        this.snapshotChangeEventSourceMetrics =
                changeEventSourceMetricsFactory.getSnapshotMetrics(
                        taskContext, queue, metadataProvider);

        this.errorHandler = new InformixErrorHandler(connectorConfig.getLogicalName(), queue);
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
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        return getDbzConnectorConfig().getTableFilters().dataCollectionFilter();
    }

    @Override
    public InformixOffset getStreamOffset(SourceRecord record) {
        return InformixUtils.createCDCOffset(record.sourceOffset());
    }

    @Override
    public InformixDatabaseSchema getDatabaseSchema() {
        return databaseSchema;
    }

    @Override
    public SeaTunnelRowType getSplitType(Table table) {
        return InformixUtils.getSplitType(table);
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public JdbcSourceEventDispatcher getDispatcher() {
        return dispatcher;
    }

    @Override
    public InformixOffsetContext getOffsetContext() {
        return offsetContext;
    }

    @Override
    public InformixSourceConfig getSourceConfig() {
        return (InformixSourceConfig) sourceConfig;
    }

    @Override
    public InformixConnectorConfig getDbzConnectorConfig() {
        return (InformixConnectorConfig) super.getDbzConnectorConfig();
    }

    private InformixOffsetContext loadStartingOffsetState(
            InformixOffsetContext.Loader loader, SourceSplitBase split) {
        Offset offset =
                split.isSnapshotSplit()
                        ? InformixOffset.INITIAL_OFFSET
                        : split.asIncrementalSplit().getStartupOffset();
        return loader.load(offset.getOffset());
    }

    private void validateAndLoadDatabaseHistory(
            InformixOffsetContext offsetContext, InformixDatabaseSchema databaseSchema) {
        databaseSchema.initializeStorage();
        databaseSchema.recover(offsetContext);
    }
}
