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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.schema.ddl;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MysqlDataTypeConvertor;

import com.mysql.cj.MysqlType;

import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MysqlDataTypeConvertor.PRECISION;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MysqlDataTypeConvertor.SCALE;

public class MysqlColumnConverter {
    private static final MysqlDataTypeConvertor MYSQL_DATA_TYPE_CONVERTOR =
            new MysqlDataTypeConvertor();

    public static Column convert(io.debezium.relational.Column column) {
        MysqlType mysqlType = MysqlType.getByName(column.typeName());
        int columnLength = column.length();
        long longColumnLength = column.length();
        long bitLength = 0;
        switch (mysqlType) {
            case CHAR:
            case VARCHAR:
                columnLength = column.length() * 3;
                longColumnLength = columnLength;
                break;
            case JSON:
                longColumnLength = 4 * 1024 * 1024 * 1024L;
                columnLength = (int) longColumnLength;
                break;
            case TINYTEXT:
                columnLength = 255;
                longColumnLength = columnLength;
                break;
            case TEXT:
            case ENUM:
                columnLength = 65535;
                longColumnLength = columnLength;
                break;
            case MEDIUMTEXT:
                columnLength = 16777215;
                longColumnLength = columnLength;
                break;
            case LONGTEXT:
                longColumnLength = 4294967295L;
                columnLength = Integer.MAX_VALUE;
                break;
            case TINYBLOB:
                bitLength = 255 * 8;
                break;
            case BLOB:
                bitLength = 65535 * 8;
                break;
            case MEDIUMBLOB:
                bitLength = 16777215 * 8;
                break;
            case LONGBLOB:
                bitLength = 4294967295L * 8;
                break;
            case BIT:
                bitLength = column.length();
                break;
            case BINARY:
            case VARBINARY:
                bitLength = column.length() * 8;
                break;
            default:
                break;
        }
        SeaTunnelDataType seaTunnelDatatype = convertDataType(column);
        return PhysicalColumn.of(
                column.name(),
                seaTunnelDatatype,
                columnLength,
                column.isOptional(),
                column.defaultValue(),
                null,
                column.typeName(),
                false,
                false,
                bitLength,
                null,
                longColumnLength);
    }

    public static SeaTunnelDataType convertDataType(io.debezium.relational.Column column) {
        MysqlType mysqlType = MysqlType.getByName(column.typeName());
        Map<String, Object> properties = new HashMap<>();
        properties.put(PRECISION, column.length());
        if (column.scale().isPresent()) {
            properties.put(SCALE, column.scale().get());
        }
        return MYSQL_DATA_TYPE_CONVERTOR.toSeaTunnelType(mysqlType, properties);
    }
}
