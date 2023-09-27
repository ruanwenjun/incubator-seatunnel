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

package org.apache.seatunnel.api.table.catalog.schema;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.utils.JsonUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Parse schema from config, the config must contain a schema option. */
public class ConfigParser implements TableSchemaParser<Config> {

    private final TableSchemaParser.FieldParser<Config> fieldParser = new FieldParser();
    private final TableSchemaParser.ColumnParser<Config> columnParser = new ColumnParser();
    private final TableSchemaParser.PrimaryKeyParser<Config> primaryKeyParser =
            new PrimaryKeyParser();
    private final TableSchemaParser.ConstraintKeyParser<Config> constraintKeyParser =
            new ConstraintKeyParser();

    /**
     * Parse schema from config, the config must contain a schema option.
     *
     * @return TableSchema
     * @throws IllegalArgumentException if config not contains schema option
     */
    @Override
    public TableSchema parse(Config schemaConfig) {
        if (schemaConfig.hasPath(TableSchemaOptions.FieldOptions.FIELDS.key())
                && schemaConfig.hasPath(TableSchemaOptions.ColumnOptions.COLUMNS.key())) {
            throw new IllegalArgumentException(
                    "Schema config can't contains both [fields] and [columns], please correct your config first");
        }

        TableSchema.Builder tableSchemaBuilder = TableSchema.builder();

        if (schemaConfig.hasPath(TableSchemaOptions.FieldOptions.FIELDS.key())) {
            tableSchemaBuilder.columns(fieldParser.parse(schemaConfig));
        }

        if (schemaConfig.hasPath(TableSchemaOptions.ColumnOptions.COLUMNS.key())) {
            tableSchemaBuilder.columns(columnParser.parse(schemaConfig));
        }

        if (schemaConfig.hasPath(TableSchemaOptions.PrimaryKeyOptions.PRIMARY_KEY.key())) {
            tableSchemaBuilder.primaryKey(primaryKeyParser.parse(schemaConfig));
        }

        if (schemaConfig.hasPath(TableSchemaOptions.ConstraintKeyOptions.CONSTRAINT_KEYS.key())) {
            tableSchemaBuilder.constraintKey(constraintKeyParser.parse(schemaConfig));
        }

        // todo: validate schema
        return tableSchemaBuilder.build();
    }

    public static class FieldParser implements TableSchemaParser.FieldParser<Config> {
        @Override
        public List<Column> parse(Config schemaConfig) {
            Config fieldConfig =
                    schemaConfig.getConfig(TableSchemaOptions.FieldOptions.FIELDS.key());
            // Because the entrySet in typesafe config couldn't keep key-value order
            // So use jackson parsing schema information into a map to keep key-value order
            ConfigRenderOptions options = ConfigRenderOptions.concise();
            String schema = fieldConfig.root().render(options);

            Map<String, String> fieldMap = JsonUtils.toLinkedHashMap(schema);

            return fieldMap.entrySet().stream()
                    .map(
                            entry -> {
                                String key = entry.getKey();
                                String value = entry.getValue();
                                SeaTunnelDataType<?> dataType =
                                        SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                                value);
                                return PhysicalColumn.of(key, dataType, 0, true, null, null);
                            })
                    .collect(Collectors.toList());
        }
    }

    public static class ColumnParser implements TableSchemaParser.ColumnParser<Config> {

        @Override
        public List<Column> parse(Config schemaConfig) {
            List<? extends Config> columnConfigList =
                    schemaConfig.getConfigList(TableSchemaOptions.ColumnOptions.COLUMNS.key());

            return columnConfigList.stream()
                    .map(
                            columnConfig -> {
                                String name;
                                if (columnConfig.hasPath(
                                        TableSchemaOptions.ColumnOptions.NAME.key())) {
                                    name =
                                            columnConfig.getString(
                                                    TableSchemaOptions.ColumnOptions.NAME.key());
                                } else {
                                    throw new IllegalArgumentException(
                                            "schema.columns.* config need option [name], please correct your config first");
                                }
                                SeaTunnelDataType<?> seaTunnelDataType;
                                if (columnConfig.hasPath(
                                        TableSchemaOptions.ColumnOptions.TYPE.key())) {
                                    seaTunnelDataType =
                                            SeaTunnelDataTypeConvertorUtil
                                                    .deserializeSeaTunnelDataType(
                                                            columnConfig.getString(
                                                                    TableSchemaOptions.ColumnOptions
                                                                            .TYPE
                                                                            .key()));
                                } else {
                                    throw new IllegalArgumentException(
                                            "schema.columns.* config need option [type], please correct your config first");
                                }
                                Integer columnLength =
                                        TypesafeConfigUtils.getConfig(
                                                columnConfig,
                                                TableSchemaOptions.ColumnOptions.COLUMN_LENGTH
                                                        .key(),
                                                TableSchemaOptions.ColumnOptions.COLUMN_LENGTH
                                                        .defaultValue());
                                boolean nullable =
                                        TypesafeConfigUtils.getConfig(
                                                columnConfig,
                                                TableSchemaOptions.ColumnOptions.NULLABLE.key(),
                                                TableSchemaOptions.ColumnOptions.NULLABLE
                                                        .defaultValue());
                                Object defaultValue = null;
                                if (columnConfig.hasPath(
                                        TableSchemaOptions.ColumnOptions.DEFAULT_VALUE.key())) {
                                    columnConfig
                                            .getValue(
                                                    TableSchemaOptions.ColumnOptions.DEFAULT_VALUE
                                                            .key())
                                            .unwrapped();
                                }
                                String comment = null;
                                if (columnConfig.hasPath(
                                        TableSchemaOptions.ColumnOptions.COMMENT.key())) {
                                    comment =
                                            columnConfig.getString(
                                                    TableSchemaOptions.ColumnOptions.COMMENT.key());
                                }

                                return PhysicalColumn.of(
                                        name,
                                        seaTunnelDataType,
                                        columnLength,
                                        nullable,
                                        defaultValue,
                                        comment);
                            })
                    .collect(Collectors.toList());
        }
    }

    public static class PrimaryKeyParser implements TableSchemaParser.PrimaryKeyParser<Config> {

        @Override
        public PrimaryKey parse(Config schemaConfig) {
            Config primaryKeyConfig =
                    schemaConfig.getConfig(TableSchemaOptions.PrimaryKeyOptions.PRIMARY_KEY.key());

            CheckResult checkResult =
                    CheckConfigUtil.checkAllExists(
                            primaryKeyConfig,
                            TableSchemaOptions.PrimaryKeyOptions.PRIMARY_KEY_NAME.key(),
                            TableSchemaOptions.PrimaryKeyOptions.PRIMARY_KEY_COLUMNS.key());
            if (!checkResult.isSuccess()) {
                throw new IllegalArgumentException(
                        "Schema config need option [primaryKey.name, primaryKey.columnNames], please correct your config first");
            }

            String primaryKeyName =
                    primaryKeyConfig.getString(
                            TableSchemaOptions.PrimaryKeyOptions.PRIMARY_KEY_NAME.key());
            List<String> columns =
                    primaryKeyConfig.getStringList(
                            TableSchemaOptions.PrimaryKeyOptions.PRIMARY_KEY_COLUMNS.key());
            return new PrimaryKey(primaryKeyName, columns);
        }
    }

    public static class ConstraintKeyParser
            implements TableSchemaParser.ConstraintKeyParser<Config> {

        @Override
        public List<ConstraintKey> parse(Config schemaConfig) {
            List<? extends Config> constraintKeysConfigs =
                    schemaConfig.getConfigList(
                            TableSchemaOptions.ConstraintKeyOptions.CONSTRAINT_KEYS.key());
            return constraintKeysConfigs.stream()
                    .map(
                            constraintKeyConfig -> {
                                String constraintName;
                                if (constraintKeyConfig.hasPath(
                                        TableSchemaOptions.ConstraintKeyOptions.CONSTRAINT_KEY_NAME
                                                .key())) {
                                    constraintName =
                                            constraintKeyConfig.getString(
                                                    TableSchemaOptions.ConstraintKeyOptions
                                                            .CONSTRAINT_KEY_NAME
                                                            .key());
                                } else {
                                    throw new IllegalArgumentException(
                                            "schema.constraintKeys.* config need option [constraintName], please correct your config first");
                                }
                                ConstraintKey.ConstraintType constraintType;
                                if (constraintKeyConfig.hasPath(
                                        TableSchemaOptions.ConstraintKeyOptions.CONSTRAINT_KEY_TYPE
                                                .key())) {
                                    constraintType =
                                            ConstraintKey.ConstraintType.valueOf(
                                                    constraintKeyConfig.getString(
                                                            TableSchemaOptions.ConstraintKeyOptions
                                                                    .CONSTRAINT_KEY_TYPE
                                                                    .key()));
                                } else {
                                    throw new IllegalArgumentException(
                                            "schema.constraintKeys.* config need option [constraintType], please correct your config first");
                                }
                                List<ConstraintKey.ConstraintKeyColumn> constraintKeyColumns =
                                        constraintKeyConfig
                                                .getConfigList(
                                                        TableSchemaOptions.ConstraintKeyOptions
                                                                .CONSTRAINT_KEY_COLUMNS
                                                                .key())
                                                .stream()
                                                .map(
                                                        columnConfig -> {
                                                            String columnName;
                                                            if (columnConfig.hasPath(
                                                                    TableSchemaOptions
                                                                            .ConstraintKeyOptions
                                                                            .CONSTRAINT_KEY_COLUMN_NAME
                                                                            .key())) {
                                                                columnName =
                                                                        columnConfig.getString(
                                                                                TableSchemaOptions
                                                                                        .ConstraintKeyOptions
                                                                                        .CONSTRAINT_KEY_COLUMN_NAME
                                                                                        .key());
                                                            } else {
                                                                throw new IllegalArgumentException(
                                                                        "schema.constraintKeys.*.columns.* config need option [columnName], please correct your config first");
                                                            }
                                                            ConstraintKey.ColumnSortType
                                                                    columnSortType;
                                                            if (columnConfig.hasPath(
                                                                    TableSchemaOptions
                                                                            .ConstraintKeyOptions
                                                                            .CONSTRAINT_KEY_COLUMN_SORT_TYPE
                                                                            .key())) {
                                                                columnSortType =
                                                                        ConstraintKey.ColumnSortType
                                                                                .valueOf(
                                                                                        columnConfig
                                                                                                .getString(
                                                                                                        TableSchemaOptions
                                                                                                                .ConstraintKeyOptions
                                                                                                                .CONSTRAINT_KEY_COLUMN_SORT_TYPE
                                                                                                                .key()));
                                                            } else {
                                                                columnSortType =
                                                                        ConstraintKey.ColumnSortType
                                                                                .ASC;
                                                            }
                                                            return ConstraintKey.ConstraintKeyColumn
                                                                    .of(columnName, columnSortType);
                                                        })
                                                .collect(Collectors.toList());
                                return ConstraintKey.of(
                                        constraintType, constraintName, constraintKeyColumns);
                            })
                    .collect(Collectors.toList());
        }
    }
}
