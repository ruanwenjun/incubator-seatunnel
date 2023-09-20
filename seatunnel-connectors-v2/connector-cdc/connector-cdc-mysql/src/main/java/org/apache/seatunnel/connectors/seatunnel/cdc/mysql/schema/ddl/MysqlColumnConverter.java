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
        SeaTunnelDataType datatype = convertDataType(column);
        long longColumnLength;
        switch (datatype.getSqlType()) {
            case STRING:
                longColumnLength = column.length() * 3;
                break;
            default:
                longColumnLength = column.length();
                break;
        }
        return PhysicalColumn.of(
                column.name(),
                datatype,
                column.length(),
                column.isOptional(),
                column.defaultValue(),
                null,
                column.typeName(),
                false,
                false,
                null,
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
