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

package org.apache.seatunnel.connectors.selectdb.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.selectdb.sink.committer.SelectDBCommitInfo;
import org.apache.seatunnel.connectors.selectdb.sink.writer.SelectDBSinkState;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.api.sink.SinkCommonOptions.MULTI_TABLE_SINK_REPLICA;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.BASE_URL;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.CLUSTER_NAME;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.CUSTOM_SQL;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.LOAD_URL;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.PASSWORD;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.SAVE_MODE;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.SAVE_MODE_CREATE_TEMPLATE;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.SELECTDB_SINK_CONFIG_PREFIX;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.SINK_BUFFER_COUNT;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.SINK_BUFFER_SIZE;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.SINK_ENABLE_DELETE;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.SINK_FLUSH_QUEUE_SIZE;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.SINK_LABEL_PREFIX;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.SINK_MAX_RETRIES;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.TABLE_IDENTIFIER;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig.USERNAME;

@AutoService(Factory.class)
public class SelectDBSinkFactory
        implements TableSinkFactory<
                SeaTunnelRow, SelectDBSinkState, SelectDBCommitInfo, SelectDBCommitInfo> {
    @Override
    public String factoryIdentifier() {
        return "SelectDBCloud";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(BASE_URL, LOAD_URL, CLUSTER_NAME, USERNAME, PASSWORD)
                .optional(
                        TABLE_IDENTIFIER,
                        SINK_MAX_RETRIES,
                        SINK_LABEL_PREFIX,
                        SINK_BUFFER_SIZE,
                        SINK_BUFFER_COUNT,
                        SINK_ENABLE_DELETE,
                        SINK_FLUSH_QUEUE_SIZE,
                        SAVE_MODE,
                        CUSTOM_SQL,
                        SAVE_MODE_CREATE_TEMPLATE,
                        SELECTDB_SINK_CONFIG_PREFIX,
                        MULTI_TABLE_SINK_REPLICA)
                .build();
    }

    @Override
    public TableSink<SeaTunnelRow, SelectDBSinkState, SelectDBCommitInfo, SelectDBCommitInfo>
            createSink(TableFactoryContext context) {
        CatalogTable catalogTable = context.getCatalogTables().get(0);
        TableIdentifier newTableId =
                TableIdentifier.of(
                        catalogTable.getTableId().getCatalogName(),
                        catalogTable.getTableId().getDatabaseName(),
                        null,
                        catalogTable.getTableId().getTableName());

        return () ->
                new SelectDBSink(CatalogTable.of(newTableId, catalogTable), context.getOptions());
    }
}
