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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.informix;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class InformixTypeMapper implements JdbcDialectTypeMapper {

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

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String name = metadata.getColumnName(colIndex);
        String type = metadata.getColumnTypeName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        return mapping(name, type, precision, scale);
    }

    public SeaTunnelDataType<?> mapping(
            String columnName, String columnType, Integer precision, Integer scale) {
        String dataType = columnType.toUpperCase();
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
                        String.format(
                                "Doesn't support Informix type '%s' on column '%s' yet",
                                columnType, columnName));
        }
    }
}
