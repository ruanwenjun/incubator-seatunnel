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

package org.apache.seatunnel.connectors.doris.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.doris.config.DorisConfig;
import org.apache.seatunnel.connectors.doris.sink.committer.DorisCommitInfo;
import org.apache.seatunnel.connectors.doris.sink.writer.DorisSinkState;
import org.apache.seatunnel.connectors.doris.util.UnsupportedTypeConverterUtils;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.seatunnel.api.sink.SinkCommonOptions.MULTI_TABLE_SINK_REPLICA;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_DATABASE_NAME_KEY;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_SCHEMA_NAME_KEY;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_TABLE_NAME_KEY;
import static org.apache.seatunnel.connectors.doris.config.DorisConfig.COLUMN_PATTERN;
import static org.apache.seatunnel.connectors.doris.config.DorisConfig.COLUMN_REPLACEMENT;
import static org.apache.seatunnel.connectors.doris.config.DorisConfig.DATABASE;
import static org.apache.seatunnel.connectors.doris.config.DorisConfig.NEEDS_UNSUPPORTED_TYPE_CASTING;
import static org.apache.seatunnel.connectors.doris.config.DorisConfig.SAVE_MODE_CREATE_TEMPLATE;
import static org.apache.seatunnel.connectors.doris.config.DorisConfig.TABLE;

@AutoService(Factory.class)
public class DorisSinkFactory implements TableSinkFactory {

    public static final String IDENTIFIER = "Doris";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        DorisConfig.BASE_URL,
                        DorisConfig.DATABASE,
                        DorisConfig.TABLE,
                        DorisConfig.FENODES,
                        DorisConfig.USERNAME,
                        DorisConfig.PASSWORD,
                        DorisConfig.SINK_LABEL_PREFIX,
                        DorisConfig.DORIS_SINK_CONFIG_PREFIX,
                        DorisConfig.DATA_SAVE_MODE,
                        DorisConfig.SCHEMA_SAVE_MODE)
                .bundled(COLUMN_PATTERN, COLUMN_REPLACEMENT)
                .optional(
                        DorisConfig.SINK_ENABLE_2PC,
                        DorisConfig.SINK_ENABLE_DELETE,
                        MULTI_TABLE_SINK_REPLICA,
                        SAVE_MODE_CREATE_TEMPLATE,
                        NEEDS_UNSUPPORTED_TYPE_CASTING)
                .conditional(
                        DorisConfig.DATA_SAVE_MODE,
                        DataSaveMode.CUSTOM_PROCESSING,
                        DorisConfig.CUSTOM_SQL)
                .build();
    }

    @Override
    public TableSink<SeaTunnelRow, DorisSinkState, DorisCommitInfo, DorisCommitInfo> createSink(
            TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        CatalogTable catalogTable =
                readonlyConfig.get(NEEDS_UNSUPPORTED_TYPE_CASTING)
                        ? UnsupportedTypeConverterUtils.convertCatalogTable(
                                context.getCatalogTable())
                        : context.getCatalogTable();
        final CatalogTable finalCatalogTable =
                this.renameCatalogTable(readonlyConfig, catalogTable);
        // rename table.identifier
        TablePath tablePath = finalCatalogTable.getTableId().toTablePath();
        String tableIdentifier = tablePath.getDatabaseName() + "." + tablePath.getTableName();
        final Map<String, Object> confData = readonlyConfig.getConfData();
        confData.put(DorisConfig.TABLE_IDENTIFIER.key(), tableIdentifier);
        return () -> new DorisSink(finalCatalogTable, ReadonlyConfig.fromMap(confData));
    }

    private CatalogTable renameCatalogTable(ReadonlyConfig options, CatalogTable catalogTable) {

        TableIdentifier tableId = catalogTable.getTableId();
        String tableName;
        String namespace;
        if (StringUtils.isNotEmpty(options.get(TABLE))) {
            tableName = replaceName(options.get(TABLE), tableId);
        } else {
            tableName = tableId.getTableName();
        }

        if (StringUtils.isNotEmpty(options.get(DATABASE))) {
            namespace = replaceName(options.get(DATABASE), tableId);
        } else {
            namespace = tableId.getSchemaName();
        }

        TableIdentifier newTableId =
                TableIdentifier.of(tableId.getCatalogName(), namespace, null, tableName);

        if (options.get(COLUMN_PATTERN) != null && options.get(COLUMN_REPLACEMENT) != null) {
            catalogTable =
                    replaceColumnName(
                            catalogTable,
                            options.get(COLUMN_PATTERN),
                            options.get(COLUMN_REPLACEMENT));
        }

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

    private CatalogTable replaceColumnName(
            CatalogTable catalogTable, String original, String replacement) {
        checkNotNull(original, "original can not be null");
        checkNotNull(replacement, "replacement can not be null");
        checkArgument(StringUtils.isNotEmpty(original), "original can not be empty");
        TableSchema tableSchema = catalogTable.getTableSchema();
        List<Column> columns = catalogTable.getTableSchema().getColumns();
        List<Column> newColumns =
                columns.stream()
                        .map(
                                column -> {
                                    if (column.getName().contains(original)) {
                                        return column.rename(
                                                column.getName().replace(original, replacement));
                                    }
                                    return column;
                                })
                        .collect(Collectors.toList());

        TableSchema newTableSchema =
                TableSchema.builder()
                        .primaryKey(tableSchema.getPrimaryKey())
                        .constraintKey(tableSchema.getConstraintKeys())
                        .columns(newColumns)
                        .build();

        return CatalogTable.of(
                catalogTable.getTableId(),
                newTableSchema,
                catalogTable.getOptions(),
                catalogTable.getPartitionKeys(),
                catalogTable.getComment());
    }
}
