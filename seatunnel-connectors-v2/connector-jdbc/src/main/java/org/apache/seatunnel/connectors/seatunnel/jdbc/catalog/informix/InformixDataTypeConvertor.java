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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.informix;

import org.apache.seatunnel.api.table.catalog.DataTypeConvertException;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.apache.commons.collections4.MapUtils;

import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@AutoService(DataTypeConvertor.class)
public class InformixDataTypeConvertor implements DataTypeConvertor<String> {

    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";
    public static final Integer DEFAULT_PRECISION = 38;

    public static final Integer DEFAULT_SCALE = 18;

    private static final String BIGINT = "BIGINT";
    private static final String BIGSERIAL = "BIGSERIAL";
    private static final String BSON = "BSON";
    private static final String BYTE = "BYTE";
    private static final String CHAR = "CHAR";
    private static final String CHARACTER = "CHARACTER";
    private static final String DATE = "DATE";
    private static final String DATETIME = "DATETIME";
    private static final String DEC = "DEC";
    private static final String DECIMAL = "DECIMAL";
    private static final String FLOAT = "FLOAT";
    private static final String INT = "INT";
    private static final String INT8 = "INT8";
    private static final String INTEGER = "INTEGER";
    private static final String INTERVAL = "INTERVAL";
    private static final String MONEY = "MONEY";
    private static final String NCHAR = "NCHAR";
    private static final String NUMERIC = "NUMERIC";
    private static final String NVARCHAR = "NVARCHAR";
    private static final String REAL = "REAL";
    private static final String SERIAL = "SERIAL";
    private static final String SERIAL8 = "SERIAL8";
    private static final String SMALLFLOAT = "SMALLFLOAT";
    private static final String SMALLINT = "SMALLINT";
    private static final String TEXT = "TEXT";
    private static final String VARCHAR = "VARCHAR";
    private static final String BOOLEAN = "BOOLEAN";

    private static final String BLOB = "BLOB";
    private static final String CLOB = "CLOB";
    private static final String LVARCHAR = "LVARCHAR";
    private static final String IDSSECURITYLABEL = "IDSSECURITYLABEL";
    private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
    // TODO "CHARACTER VARYING"

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String connectorDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        checkNotNull(connectorDataType, "seaTunnelDataType cannot be null");
        String dataType = connectorDataType.toUpperCase();
        if (dataType.startsWith(DATETIME)) {
            return LocalTimeType.LOCAL_DATE_TIME_TYPE;
        }
        switch (dataType) {
            case SMALLINT:
                return BasicType.SHORT_TYPE;
            case BIGINT:
            case BIGSERIAL:
            case INT8:
            case SERIAL8:
                return BasicType.LONG_TYPE;
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case BSON:
            case CHAR:
            case CHARACTER:
            case NCHAR:
            case NVARCHAR:
            case TEXT:
            case VARCHAR:
            case LVARCHAR:
            case IDSSECURITYLABEL:
                return BasicType.STRING_TYPE;
            case BYTE:
            case BLOB:
            case CLOB:
                return PrimitiveByteArrayType.INSTANCE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case DATETIME:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case DEC:
            case DECIMAL:
            case MONEY:
            case NUMERIC:
                int precision =
                        MapUtils.getInteger(dataTypeProperties, PRECISION, DEFAULT_PRECISION);
                int scale = MapUtils.getInteger(dataTypeProperties, SCALE, DEFAULT_SCALE);
                return new DecimalType(precision, scale);
            case FLOAT:
            case REAL:
            case SMALLFLOAT:
                return BasicType.FLOAT_TYPE;
            case DOUBLE_PRECISION:
                return BasicType.DOUBLE_TYPE;
            case INT:
            case INTEGER:
            case SERIAL:
                return BasicType.INT_TYPE;
            case INTERVAL:
            default:
                throw new JdbcConnectorException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format("Doesn't support Informix type '%s' yet", connectorDataType));
        }
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String connectorDataType) {
        return toSeaTunnelType(connectorDataType, new HashMap<>(0));
    }

    @Override
    public String toConnectorType(
            SeaTunnelDataType<?> seaTunnelDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        checkNotNull(seaTunnelDataType, "seaTunnelDataType cannot be null");
        SqlType sqlType = seaTunnelDataType.getSqlType();
        switch (sqlType) {
            case TINYINT:
            case SMALLINT:
                return SMALLINT;
            case INT:
                return INT;
            case BIGINT:
                return BIGINT;
            case DECIMAL:
                return DECIMAL;
            case FLOAT:
                return FLOAT;
            case DOUBLE:
                return DOUBLE_PRECISION;
            case BOOLEAN:
                return BOOLEAN;
            case STRING:
                return VARCHAR;
            case DATE:
                return DATE;
            case BYTES:
                return BLOB;
            case TIMESTAMP:
                return DATETIME;
            case TIME:
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support SeaTunnel type '%s''  yet.", seaTunnelDataType));
        }
    }

    @Override
    public String getIdentity() {
        return DatabaseIdentifier.INFORMIX;
    }
}
