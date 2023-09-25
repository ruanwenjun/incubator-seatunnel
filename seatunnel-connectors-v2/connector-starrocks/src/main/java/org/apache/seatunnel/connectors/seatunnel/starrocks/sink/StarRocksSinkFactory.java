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

package org.apache.seatunnel.connectors.seatunnel.starrocks.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksOptions;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksSinkOptions;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_DATABASE_NAME_KEY;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_SCHEMA_NAME_KEY;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_TABLE_NAME_KEY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksSinkOptions.DATA_SAVE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksSinkOptions.SCHEMA_SAVE_MODE;

@AutoService(Factory.class)
public class StarRocksSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "StarRocks";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(StarRocksOptions.USERNAME, StarRocksOptions.PASSWORD)
                .required(StarRocksSinkOptions.DATABASE, StarRocksOptions.BASE_URL)
                .required(StarRocksSinkOptions.NODE_URLS)
                .required(SCHEMA_SAVE_MODE)
                .required(DATA_SAVE_MODE)
                .optional(
                        StarRocksSinkOptions.TABLE,
                        StarRocksSinkOptions.LABEL_PREFIX,
                        StarRocksSinkOptions.BATCH_MAX_SIZE,
                        StarRocksSinkOptions.BATCH_MAX_BYTES,
                        StarRocksSinkOptions.BATCH_INTERVAL_MS,
                        StarRocksSinkOptions.MAX_RETRIES,
                        StarRocksSinkOptions.MAX_RETRY_BACKOFF_MS,
                        StarRocksSinkOptions.RETRY_BACKOFF_MULTIPLIER_MS,
                        StarRocksSinkOptions.STARROCKS_CONFIG,
                        StarRocksSinkOptions.ENABLE_UPSERT_DELETE,
                        StarRocksSinkOptions.SAVE_MODE_CREATE_TEMPLATE)
                .conditional(
                        DATA_SAVE_MODE,
                        DataSaveMode.CUSTOM_PROCESSING,
                        StarRocksSinkOptions.CUSTOM_SQL)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        SinkConfig sinkConfig = SinkConfig.of(context.getOptions());
        CatalogTable catalogTable = context.getCatalogTable();
        if (StringUtils.isBlank(sinkConfig.getTable())) {
            sinkConfig.setTable(catalogTable.getTableId().getTableName());
        }
        // get source table relevant information
        TableIdentifier tableId = catalogTable.getTableId();
        String sourceDatabaseName = tableId.getDatabaseName();
        String sourceSchemaName = tableId.getSchemaName();
        String sourceTableName = tableId.getTableName();
        // get sink table relevant information
        String sinkDatabaseName = sinkConfig.getDatabase();
        String sinkTableNameBefore = sinkConfig.getTable();
        if (StringUtils.isEmpty(sinkTableNameBefore)) {
            sinkTableNameBefore = REPLACE_TABLE_NAME_KEY;
        }
        String[] sinkTableSplitArray = sinkTableNameBefore.split("\\.");
        String sinkTableName = sinkTableSplitArray[sinkTableSplitArray.length - 1];
        String sinkSchemaName;
        if (sinkTableSplitArray.length > 1) {
            sinkSchemaName = sinkTableSplitArray[sinkTableSplitArray.length - 2];
        } else {
            sinkSchemaName = null;
        }
        // to replace
        String finalDatabaseName =
                sinkDatabaseName.replace(REPLACE_DATABASE_NAME_KEY, sourceDatabaseName);
        String finalSchemaName;
        if (sinkSchemaName != null) {
            if (sourceSchemaName == null) {
                finalSchemaName = sinkSchemaName;
            } else {
                finalSchemaName = sinkSchemaName.replace(REPLACE_SCHEMA_NAME_KEY, sourceSchemaName);
            }
        } else {
            finalSchemaName = null;
        }
        String finalTableName = sinkTableName.replace(REPLACE_TABLE_NAME_KEY, sourceTableName);
        // rebuild TableIdentifier and catalogTable
        TableIdentifier newTableId =
                TableIdentifier.of(
                        tableId.getCatalogName(),
                        finalDatabaseName,
                        finalSchemaName,
                        finalTableName);
        catalogTable =
                CatalogTable.of(
                        newTableId,
                        catalogTable.getTableSchema(),
                        catalogTable.getOptions(),
                        catalogTable.getPartitionKeys(),
                        catalogTable.getCatalogName());

        CatalogTable finalCatalogTable = catalogTable;
        // reset
        sinkConfig.setTable(finalTableName);
        sinkConfig.setDatabase(finalDatabaseName);
        // get saveMode
        DataSaveMode dataSaveMode = sinkConfig.getDataSaveMode();
        SchemaSaveMode schemaSaveMode = sinkConfig.getSchemaSaveMode();
        return () ->
                new StarRocksSink(
                        sinkConfig,
                        finalCatalogTable,
                        context.getOptions(),
                        schemaSaveMode,
                        dataSaveMode);
    }
}
