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

package org.apache.seatunnel.connectors.seatunnel.redshift.datatype;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.io.Serializable;

public class ToRedshiftTypeConverter implements Serializable {
    public static final ToRedshiftTypeConverter INSTANCE = new ToRedshiftTypeConverter();

    private static final String SMALLINT = "SMALLINT";
    private static final String INT = "INT";
    private static final String BIGINT = "BIGINT";
    private static final String FLOAT4 = "FLOAT4";
    private static final String FLOAT8 = "FLOAT8";
    private static final String BOOLEAN = "BOOLEAN";
    private static final String VARCHAR = "VARCHAR";
    private static final String MAX_LENGTH_VARCHAR = "VARCHAR(65535)";
    private static final String VARBINARY = "VARBINARY";
    private static final String DATE = "DATE";
    private static final String TIMESTAMP = "TIMESTAMP";
    private static final String SUPER = "SUPER";

    public String convert(Column column) {
        switch (column.getDataType().getSqlType()) {
            case STRING:
                if (column.getColumnLength() != null && column.getColumnLength() >= 65535) {
                    return SUPER;
                }
                if (column.getLongColumnLength() != null && column.getLongColumnLength() >= 65535) {
                    return SUPER;
                }
                if (column.getLongColumnLength() != null && column.getLongColumnLength() != 0) {
                    return VARCHAR + "(" + column.getLongColumnLength() + ")";
                }
                return MAX_LENGTH_VARCHAR;
            default:
                return convert(column.getDataType());
        }
    }

    public String convert(SeaTunnelDataType dataType) {
        switch (dataType.getSqlType()) {
            case TINYINT:
            case SMALLINT:
                return SMALLINT;
            case INT:
                return INT;
            case BIGINT:
                return BIGINT;
            case DECIMAL:
                return String.format(
                        "DECIMAL(%s,%s)",
                        ((DecimalType) dataType).getPrecision(),
                        ((DecimalType) dataType).getScale());
            case FLOAT:
                return FLOAT4;
            case DOUBLE:
                return FLOAT8;
            case BOOLEAN:
                return BOOLEAN;
            case STRING:
                return MAX_LENGTH_VARCHAR;
            case BYTES:
                return VARBINARY;
            case DATE:
                return DATE;
            case TIMESTAMP:
                return TIMESTAMP;
            case MAP:
            case ARRAY:
            case ROW:
                return SUPER;
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }
}
