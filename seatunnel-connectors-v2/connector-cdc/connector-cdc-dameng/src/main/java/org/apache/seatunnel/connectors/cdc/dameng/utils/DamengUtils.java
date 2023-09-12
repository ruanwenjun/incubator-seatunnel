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

package org.apache.seatunnel.connectors.cdc.dameng.utils;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.dameng.source.offset.LogMinerOffset;

import io.debezium.connector.dameng.DamengConnection;
import io.debezium.connector.dameng.DamengConnectorConfig;
import io.debezium.connector.dameng.DamengDatabaseSchema;
import io.debezium.connector.dameng.DamengTopicSelector;
import io.debezium.connector.dameng.DamengValueConverters;
import io.debezium.connector.dameng.Scn;
import io.debezium.connector.dameng.SourceInfo;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DamengUtils {

    public static DamengDatabaseSchema createDamengDatabaseSchema(
            DamengConnectorConfig dbzConfig, DamengConnection connection) {
        TopicSelector<TableId> topicSelector = DamengTopicSelector.defaultSelector(dbzConfig);
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();
        DamengValueConverters valueConverters = new DamengValueConverters(dbzConfig, connection);

        return new DamengDatabaseSchema(
                dbzConfig, valueConverters, topicSelector, schemaNameAdjuster, false);
    }

    public static SeaTunnelRowType getSplitType(Table table) {
        List<Column> primaryKeys = table.primaryKeyColumns();
        if (primaryKeys.isEmpty()) {
            throw new SeaTunnelException(
                    String.format(
                            "Incremental snapshot for tables requires primary key,"
                                    + " but table %s doesn't have primary key.",
                            table.id()));
        }

        // use first field in primary key as the split key
        return convert(primaryKeys.get(0));
    }

    public static SeaTunnelRowType convert(Table table) {
        return convert(table.columns().toArray(new Column[0]));
    }

    public static SeaTunnelRowType convert(Column... columns) {
        String[] fieldNames = new String[columns.length];
        SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType[columns.length];
        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i];
            fieldNames[i] = column.name();
            fieldTypes[i] = DamengTypeConverter.convert(column);
        }
        return new SeaTunnelRowType(fieldNames, fieldTypes);
    }

    public static LogMinerOffset createLogMinerOffset(Map<String, ?> offset) {
        Map<String, String> offsetStrMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offset.entrySet()) {
            offsetStrMap.put(
                    entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
        }
        return new LogMinerOffset(Scn.valueOf(offsetStrMap.get(SourceInfo.SCN_KEY)));
    }
}
