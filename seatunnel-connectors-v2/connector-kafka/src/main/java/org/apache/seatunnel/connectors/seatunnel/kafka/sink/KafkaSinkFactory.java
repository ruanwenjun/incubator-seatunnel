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

package org.apache.seatunnel.connectors.seatunnel.kafka.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.Config;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.MessageFormat;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

import java.util.Arrays;
import java.util.Map;

import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_DATABASE_NAME_KEY;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_SCHEMA_NAME_KEY;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_TABLE_NAME_KEY;

@AutoService(Factory.class)
public class KafkaSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Kafka";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        Config.FORMAT,
                        Config.BOOTSTRAP_SERVERS,
                        Config.SCHEMA_SAVE_MODE,
                        Config.DATA_SAVE_MODE)
                .conditional(
                        Config.FORMAT,
                        Arrays.asList(
                                MessageFormat.JSON, MessageFormat.CANAL_JSON, MessageFormat.TEXT),
                        Config.TOPIC)
                .conditional(
                        Config.SCHEMA_SAVE_MODE,
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        Config.TOPIC_PARTITIONS_NUM)
                .conditional(
                        Config.SCHEMA_SAVE_MODE,
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        Config.TOPIC_REPLICATION_NUM)
                .optional(
                        Config.KAFKA_CONFIG,
                        Config.ASSIGN_PARTITIONS,
                        Config.TRANSACTION_PREFIX,
                        Config.SEMANTICS,
                        Config.PARTITION,
                        Config.PARTITION_KEY_FIELDS)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        CatalogTable catalogTable = context.getCatalogTable();
        ReadonlyConfig options = context.getOptions();
        TableIdentifier tableId = catalogTable.getTableId();
        Map<String, Object> confData = options.getConfData();
        // get source database schema table
        String sourceDatabase = tableId.getDatabaseName();
        String sourceSchema = tableId.getSchemaName();
        String sourceTableName = tableId.getTableName();
        // replace topic name
        String topic = options.get(Config.TOPIC);
        if (StringUtils.isNotEmpty(topic)) {
            if (sourceDatabase != null) {
                topic = topic.replace(REPLACE_DATABASE_NAME_KEY, sourceDatabase);
            }
            if (sourceSchema != null) {
                topic = topic.replace(REPLACE_SCHEMA_NAME_KEY, sourceSchema);
            }
            if (sourceTableName != null) {
                topic = topic.replace(REPLACE_TABLE_NAME_KEY, sourceTableName);
            }
            // rebuild
            confData.put(Config.TOPIC.key(), topic);
        }
        final ReadonlyConfig finalOptions = ReadonlyConfig.fromMap(confData);
        return () ->
                new KafkaSink(
                        finalOptions,
                        context.getCatalogTable().getTableSchema().toPhysicalRowDataType());
    }
}
