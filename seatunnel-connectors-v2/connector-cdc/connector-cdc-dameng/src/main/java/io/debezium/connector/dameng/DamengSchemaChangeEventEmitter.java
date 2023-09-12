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

package io.debezium.connector.dameng;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;

import java.time.Instant;

public class DamengSchemaChangeEventEmitter implements SchemaChangeEventEmitter {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(DamengSchemaChangeEventEmitter.class);

    private final DamengOffsetContext offsetContext;
    private final TableId tableId;
    private final DamengDatabaseSchema schema;
    private final Instant changeTime;
    private final String sourceDatabaseName;
    private final String objectOwner;
    private final String ddlText;
    private final TableFilter filters;
    private final DamengStreamingChangeEventSourceMetrics streamingMetrics;

    public DamengSchemaChangeEventEmitter(
            DamengConnectorConfig connectorConfig,
            DamengOffsetContext offsetContext,
            TableId tableId,
            String sourceDatabaseName,
            String objectOwner,
            String ddlText,
            DamengDatabaseSchema schema,
            Instant changeTime,
            DamengStreamingChangeEventSourceMetrics streamingMetrics) {
        this.offsetContext = offsetContext;
        this.tableId = tableId;
        this.sourceDatabaseName = sourceDatabaseName;
        this.objectOwner = objectOwner;
        this.ddlText = ddlText;
        this.schema = schema;
        this.changeTime = changeTime;
        this.streamingMetrics = streamingMetrics;
        this.filters = connectorConfig.getTableFilters().dataCollectionFilter();
    }

    @Override
    public void emitSchemaChangeEvent(Receiver receiver) throws InterruptedException {}
}
