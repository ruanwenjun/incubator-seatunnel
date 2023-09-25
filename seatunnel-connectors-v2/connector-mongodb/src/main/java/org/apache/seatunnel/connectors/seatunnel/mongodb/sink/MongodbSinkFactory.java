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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.SinkCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.DocumentBulk;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.MongodbAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.MongodbCommitInfo;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_DATABASE_NAME_KEY;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_SCHEMA_NAME_KEY;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_TABLE_NAME_KEY;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.CONNECTOR_IDENTITY;

@AutoService(Factory.class)
public class MongodbSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(MongodbConfig.URI, MongodbConfig.DATABASE, MongodbConfig.COLLECTION)
                .optional(
                        MongodbConfig.BUFFER_FLUSH_INTERVAL,
                        MongodbConfig.BUFFER_FLUSH_MAX_ROWS,
                        MongodbConfig.RETRY_MAX,
                        MongodbConfig.RETRY_INTERVAL,
                        MongodbConfig.UPSERT_ENABLE,
                        MongodbConfig.PRIMARY_KEY,
                        SinkCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .optional(MongodbConfig.DATA_SAVE_MODE, MongodbConfig.SCHEMA_SAVE_MODE)
                .build();
    }

    @Override
    public TableSink<SeaTunnelRow, DocumentBulk, MongodbCommitInfo, MongodbAggregatedCommitInfo>
            createSink(TableSinkFactoryContext context) {
        CatalogTable catalogTable = context.getCatalogTable();
        ReadonlyConfig options = context.getOptions();
        return () -> new MongodbSink(renameCatalogTable(options, catalogTable), options);
    }

    private CatalogTable renameCatalogTable(ReadonlyConfig options, CatalogTable catalogTable) {

        TableIdentifier tableId = catalogTable.getTableId();
        String tableName;
        String namespace;
        if (StringUtils.isNotEmpty(options.get(MongodbConfig.COLLECTION))) {
            tableName = replaceName(options.get(MongodbConfig.COLLECTION), tableId);
        } else {
            tableName = tableId.getTableName();
        }

        if (StringUtils.isNotEmpty(options.get(MongodbConfig.DATABASE))) {
            namespace = replaceName(options.get(MongodbConfig.DATABASE), tableId);
        } else {
            namespace = tableId.getSchemaName();
        }

        TableIdentifier newTableId =
                TableIdentifier.of(tableId.getCatalogName(), namespace, null, tableName);

        return CatalogTable.of(newTableId, catalogTable);
    }

    private String replaceName(String original, TableIdentifier tableId) {
        if (tableId.getTableName() != null) {
            original = original.replace(REPLACE_TABLE_NAME_KEY, tableId.getTableName());
        }
        if (tableId.getSchemaName() != null) {
            original = original.replace(REPLACE_SCHEMA_NAME_KEY, tableId.getSchemaName());
        }
        if (tableId.getDatabaseName() != null) {
            original = original.replace(REPLACE_DATABASE_NAME_KEY, tableId.getDatabaseName());
        }
        return original;
    }
}
