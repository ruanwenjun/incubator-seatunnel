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

package org.apache.seatunnel.connectors.seatunnel.mongodb.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.mongodb.catalog.MongoDBCatalog;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbCollectionProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.RowDataDocumentSerializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.RowDataToBsonConverters;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.commit.MongodbSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.DocumentBulk;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.MongodbAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.MongodbCommitInfo;

import com.google.auto.service.AutoService;

import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.CONNECTOR_IDENTITY;

@AutoService(SeaTunnelSink.class)
public class MongodbSink
        implements SeaTunnelSink<
                        SeaTunnelRow, DocumentBulk, MongodbCommitInfo, MongodbAggregatedCommitInfo>,
                SupportSaveMode,
                SupportMultiTableSink {

    private MongodbWriterOptions options;

    private CatalogTable catalogTable;

    public MongodbSink() {}

    public MongodbSink(CatalogTable catalogTable, ReadonlyConfig options) {
        this.options = MongodbWriterOptions.from(options);
        this.catalogTable = catalogTable;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.options = MongodbWriterOptions.from(ReadonlyConfig.fromConfig(pluginConfig));
    }

    @Override
    public String getPluginName() {
        return CONNECTOR_IDENTITY;
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.catalogTable =
                CatalogTableUtil.getCatalogTable(
                        "mongodb",
                        options.getDatabase(),
                        null,
                        options.getCollection(),
                        seaTunnelRowType);
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.catalogTable.getTableSchema().toPhysicalRowDataType();
    }

    @Override
    public SinkWriter<SeaTunnelRow, MongodbCommitInfo, DocumentBulk> createWriter(
            SinkWriter.Context context) {
        return new MongodbWriter(
                this.catalogTable,
                new RowDataDocumentSerializer(
                        RowDataToBsonConverters.createConverter(
                                this.catalogTable.getTableSchema().toPhysicalRowDataType()),
                        options,
                        new MongoKeyExtractor(
                                options.getPrimaryKey() != null
                                        ? options.getPrimaryKey()
                                        : catalogTable
                                                .getTableSchema()
                                                .getPrimaryKey()
                                                .getColumnNames()
                                                .toArray(new String[0]))),
                options,
                context);
    }

    @Override
    public Optional<Serializer<DocumentBulk>> getWriterStateSerializer() {
        return options.transaction ? Optional.of(new DefaultSerializer<>()) : Optional.empty();
    }

    @Override
    public Optional<SinkAggregatedCommitter<MongodbCommitInfo, MongodbAggregatedCommitInfo>>
            createAggregatedCommitter() {
        return options.transaction
                ? Optional.of(new MongodbSinkAggregatedCommitter(catalogTable, options))
                : Optional.empty();
    }

    @Override
    public Optional<Serializer<MongodbAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return options.transaction ? Optional.of(new DefaultSerializer<>()) : Optional.empty();
    }

    @Override
    public Optional<Serializer<MongodbCommitInfo>> getCommitInfoSerializer() {
        return options.transaction ? Optional.of(new DefaultSerializer<>()) : Optional.empty();
    }

    @Override
    public SaveModeHandler getSaveModeHandler() {

        MongodbClientProvider clientProvider =
                MongodbCollectionProvider.builder()
                        .connectionString(options.getConnectString())
                        .database(catalogTable.getTableId().getDatabaseName())
                        .collection(catalogTable.getTableId().getTableName())
                        .build();

        MongoDBCatalog catalog = new MongoDBCatalog(clientProvider);

        return new DefaultSaveModeHandler(
                options.getSchemaSaveMode(),
                options.getDataSaveMode(),
                catalog,
                catalogTable,
                null);
    }
}
