/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hive.catalog;

import org.apache.seatunnel.api.table.catalog.DataTypeConvertException;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@AutoService(DataTypeConvertor.class)
public class HiveDataTypeConvertor implements DataTypeConvertor<String> {

    private static final Logger LOG = LoggerFactory.getLogger(HiveDataTypeConvertor.class);

    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";

    public static final Integer DEFAULT_PRECISION = 38;

    public static final Integer DEFAULT_SCALE = 18;

    // 基本数据类型
    public static final String HIVE_TINYINT = "tinyint";
    public static final String HIVE_SMALLINT = "smallint";
    public static final String HIVE_INT = "int";
    public static final String HIVE_BIGINT = "bigint";
    public static final String HIVE_FLOAT = "float";
    public static final String HIVE_DOUBLE = "double";
    public static final String HIVE_BOOLEAN = "boolean";
    public static final String HIVE_STRING = "string";
    public static final String HIVE_BINARY = "binary";
    public static final String HIVE_TIMESTAMP = "timestamp";
    public static final String HIVE_DATE = "date";
    public static final String HIVE_DECIMAL = "decimal";
    // 字符类型
    public static final String HIVE_CHAR = "char";
    public static final String HIVE_VARCHAR = "varchar";

    // 复杂数据类型
    public static final String HIVE_ARRAY = "array";
    public static final String HIVE_MAP = "map";
    public static final String HIVE_STRUCT = "struct";
    public static final String HIVE_UNION = "union";

    // 集合类型
    public static final String HIVE_ARRAY_INT = "array<int>";
    public static final String HIVE_MAP_STRING_INT = "map<string,int>";
    public static final String HIVE_STRUCT_NAME_AGE = "struct<name:string,age:int>";

    // 自定义类型
    public static final String HIVE_CUSTOM_TYPE = "custom_type";

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String connectorDataType) {
        return toSeaTunnelType(connectorDataType, new HashMap<>(0));
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String connectorDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        checkNotNull(connectorDataType, "Postgres Type cannot be null");

        switch (connectorDataType) {
            case HIVE_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case HIVE_TINYINT:
                return BasicType.BYTE_TYPE;
            case HIVE_SMALLINT:
                return BasicType.SHORT_TYPE;
            case HIVE_INT:
                return BasicType.INT_TYPE;
            case HIVE_BIGINT:
                return BasicType.LONG_TYPE;
            case HIVE_FLOAT:
                return BasicType.FLOAT_TYPE;
            case HIVE_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case HIVE_STRING:
            case HIVE_VARCHAR:
            case HIVE_STRUCT:
            case HIVE_ARRAY:
            case HIVE_MAP:
            case HIVE_UNION:
            case HIVE_ARRAY_INT:
            case HIVE_MAP_STRING_INT:
            case HIVE_STRUCT_NAME_AGE:
            case HIVE_CUSTOM_TYPE:
                return BasicType.STRING_TYPE;
            case HIVE_BINARY:
                return PrimitiveByteArrayType.INSTANCE;
            case HIVE_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case HIVE_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case HIVE_DECIMAL:
                return new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
                // 添加其他 Hive 数据类型的转换逻辑
            default:
                throw new UnsupportedOperationException(
                        String.format("Doesn't support HIVE type '%s''  yet.", connectorDataType));
        }
    }

    @Override
    public String toConnectorType(
            SeaTunnelDataType<?> seaTunnelDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        checkNotNull(seaTunnelDataType, "seaTunnelDataType cannot be null");
        SqlType sqlType = seaTunnelDataType.getSqlType();
        // todo: verify
        switch (sqlType) {
            case ARRAY:
                return HIVE_ARRAY;
            case MAP:
            case ROW:
            case STRING:
            case NULL:
                return HIVE_STRING;
            case BOOLEAN:
                return HIVE_BOOLEAN;
            case TINYINT:
                return HIVE_TINYINT;
            case SMALLINT:
                return HIVE_SMALLINT;
            case INT:
                return HIVE_INT;
            case BIGINT:
                return HIVE_BIGINT;
            case FLOAT:
                return HIVE_FLOAT;
            case DOUBLE:
                return HIVE_DOUBLE;
            case DECIMAL:
                return HIVE_DECIMAL;
            case BYTES:
                return HIVE_BINARY;
            case DATE:
                return HIVE_DATE;
            case TIME:
            case TIMESTAMP:
                return HIVE_TIMESTAMP;
            default:
                throw new UnsupportedOperationException(
                        String.format("Doesn't support HIVE type '%s''  yet.", sqlType));
        }
    }

    @Override
    public String getIdentity() {
        return "Hive";
    }
}
