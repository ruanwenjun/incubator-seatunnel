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

package org.apache.seatunnel.connectors.dolphindb.utils;

import org.apache.seatunnel.api.sink.SaveModeConstants;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;

import org.apache.commons.lang3.StringUtils;

import com.xxdb.data.Entity;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class DolphinDBSaveModeUtil {

    public static String fillingCreateSql(
            String template, String database, String table, TableSchema tableSchema) {
        String primaryKey = "";
        if (tableSchema.getPrimaryKey() != null) {
            primaryKey = String.join(",", tableSchema.getPrimaryKey().getColumnNames());
        }
        template =
                template.replaceAll(
                        String.format("\\$\\{%s\\}", SaveModeConstants.ROWTYPE_PRIMARY_KEY),
                        primaryKey);
        Map<String, CreateTableParser.ColumnInfo> columnInTemplate =
                CreateTableParser.getColumnList(template);
        template = mergeColumnInTemplate(columnInTemplate, tableSchema, template);

        String rowTypeFields =
                tableSchema.getColumns().stream()
                        .filter(column -> !columnInTemplate.containsKey(column.getName()))
                        .map(DolphinDBSaveModeUtil::columnToDolphinDBType)
                        .collect(Collectors.joining(",\n"));
        return template.replaceAll(
                        String.format("\\$\\{%s\\}", SaveModeConstants.DATABASE), database)
                .replaceAll(String.format("\\$\\{%s\\}", SaveModeConstants.TABLE_NAME), table)
                .replaceAll(
                        String.format("\\$\\{%s\\}", SaveModeConstants.ROWTYPE_FIELDS),
                        rowTypeFields);
    }

    private static String columnToDolphinDBType(Column column) {
        checkNotNull(column, "The column is required.");
        return String.format(
                "%s %s", column.getName(), dataTypeToDolphinDBType(column.getDataType()));
    }

    private static String mergeColumnInTemplate(
            Map<String, CreateTableParser.ColumnInfo> columnInTemplate,
            TableSchema tableSchema,
            String template) {
        int offset = 0;
        Map<String, Column> columnMap =
                tableSchema.getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, Function.identity()));
        for (String col : columnInTemplate.keySet()) {
            CreateTableParser.ColumnInfo columnInfo = columnInTemplate.get(col);
            if (StringUtils.isEmpty(columnInfo.getInfo())) {
                if (columnMap.containsKey(col)) {
                    Column column = columnMap.get(col);
                    String newCol = columnToDolphinDBType(column);
                    String prefix = template.substring(0, columnInfo.getStartIndex() + offset);
                    String suffix = template.substring(offset + columnInfo.getEndIndex());
                    if (prefix.endsWith("`")) {
                        prefix = prefix.substring(0, prefix.length() - 1);
                        offset--;
                    }
                    if (suffix.startsWith("`")) {
                        suffix = suffix.substring(1);
                        offset--;
                    }
                    template = prefix + newCol + suffix;
                    offset += newCol.length() - columnInfo.getName().length();
                } else {
                    throw new IllegalArgumentException("Can't find column " + col + " in table.");
                }
            }
        }
        return template;
    }

    private static String dataTypeToDolphinDBType(SeaTunnelDataType<?> dataType) {
        checkNotNull(dataType, "The SeaTunnel's data type is required.");
        switch (dataType.getSqlType()) {
            case NULL:
                return Entity.DATA_TYPE.DT_VOID.getName();
            case TIME:
                return Entity.DATA_TYPE.DT_TIME.getName();
            case STRING:
                return Entity.DATA_TYPE.DT_STRING.getName();
            case BYTES:
                return Entity.DATA_TYPE.DT_BYTE_ARRAY.getName();
            case BOOLEAN:
                return Entity.DATA_TYPE.DT_BOOL.getName();
            case TINYINT:
                return Entity.DATA_TYPE.DT_INT.getName();
            case SMALLINT:
                return Entity.DATA_TYPE.DT_INT.getName();
            case INT:
                return Entity.DATA_TYPE.DT_INT.getName();
            case BIGINT:
                return Entity.DATA_TYPE.DT_LONG.getName();
            case FLOAT:
                return Entity.DATA_TYPE.DT_FLOAT.getName();
            case DOUBLE:
                return Entity.DATA_TYPE.DT_DOUBLE.getName();
            case DATE:
                return Entity.DATA_TYPE.DT_DATE.getName();
            case TIMESTAMP:
                return Entity.DATA_TYPE.DT_TIMESTAMP.getName();
            case ARRAY:
                BasicType<?> elementType = ((ArrayType<?, ?>) dataType).getElementType();
                if (elementType.getSqlType() == SqlType.INT) {
                    return Entity.DATA_TYPE.DT_INT_ARRAY.getName();
                }
                if (elementType.getSqlType() == SqlType.BIGINT) {
                    return Entity.DATA_TYPE.DT_LONG_ARRAY.getName();
                }
                if (elementType.getSqlType() == SqlType.DOUBLE) {
                    return Entity.DATA_TYPE.DT_DOUBLE_ARRAY.getName();
                }
                if (elementType.getSqlType() == SqlType.STRING) {
                    return Entity.DATA_TYPE.DT_STRING_ARRAY.getName();
                }
                if (elementType.getSqlType() == SqlType.BOOLEAN) {
                    return Entity.DATA_TYPE.DT_BOOL_ARRAY.getName();
                }
                if (elementType.getSqlType() == SqlType.DATE) {
                    return Entity.DATA_TYPE.DT_DATE_ARRAY.getName();
                }
                if (elementType.getSqlType() == SqlType.TIMESTAMP) {
                    return Entity.DATA_TYPE.DT_TIMESTAMP_ARRAY.getName();
                }
                if (elementType.getSqlType() == SqlType.TIME) {
                    return Entity.DATA_TYPE.DT_TIME_ARRAY.getName();
                }
                if (elementType.getSqlType() == SqlType.BYTES) {
                    return Entity.DATA_TYPE.DT_BYTE_ARRAY.getName();
                }
                if (elementType.getSqlType() == SqlType.DECIMAL) {
                    return Entity.DATA_TYPE.DT_DOUBLE_ARRAY.getName();
                }
                if (elementType.getSqlType() == SqlType.ROW) {
                    return Entity.DATA_TYPE.DT_STRING_ARRAY.getName();
                }
                if (elementType.getSqlType() == SqlType.MAP) {
                    return Entity.DATA_TYPE.DT_STRING_ARRAY.getName();
                }
                throw new IllegalArgumentException(
                        "Unsupported SeaTunnel's data type: " + dataType);
            case DECIMAL:
                return Entity.DATA_TYPE.DT_DOUBLE.getName();
            case MAP:
            case ROW:
                return Entity.DATA_TYPE.DT_STRING.getName();
            default:
        }
        throw new IllegalArgumentException("Unsupported SeaTunnel's data type: " + dataType);
    }
}
