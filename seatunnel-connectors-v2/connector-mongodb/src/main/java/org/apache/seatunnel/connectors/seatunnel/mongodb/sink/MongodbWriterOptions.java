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

package org.apache.seatunnel.connectors.seatunnel.mongodb.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig;

import lombok.Getter;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.COLLECTION;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.URI;

@Getter
public class MongodbWriterOptions implements Serializable {

    private static final long serialVersionUID = 1;

    protected final String connectString;

    protected final String database;

    protected final String collection;

    protected final int flushSize;

    protected final long batchIntervalMs;

    protected final boolean upsertEnable;

    protected final String[] primaryKey;

    protected final int retryMax;

    protected final long retryInterval;

    protected final boolean transaction;

    protected final SchemaSaveMode schemaSaveMode;

    protected final DataSaveMode dataSaveMode;

    public MongodbWriterOptions(
            String connectString,
            String database,
            String collection,
            int flushSize,
            long batchIntervalMs,
            boolean upsertEnable,
            String[] primaryKey,
            int retryMax,
            long retryInterval,
            boolean transaction,
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode) {
        this.connectString = connectString;
        this.database = database;
        this.collection = collection;
        this.flushSize = flushSize;
        this.batchIntervalMs = batchIntervalMs;
        this.upsertEnable = upsertEnable;
        this.primaryKey = primaryKey;
        this.retryMax = retryMax;
        this.retryInterval = retryInterval;
        this.transaction = transaction;
        this.schemaSaveMode = schemaSaveMode;
        this.dataSaveMode = dataSaveMode;
    }

    public static MongodbWriterOptions from(ReadonlyConfig options) {
        return new MongodbWriterOptions(
                options.get(URI),
                options.get(DATABASE),
                options.get(COLLECTION),
                options.get(MongodbConfig.BUFFER_FLUSH_MAX_ROWS),
                options.get(MongodbConfig.BUFFER_FLUSH_INTERVAL),
                options.get(MongodbConfig.UPSERT_ENABLE),
                options.get(MongodbConfig.PRIMARY_KEY) == null
                        ? null
                        : options.get(MongodbConfig.PRIMARY_KEY).toArray(new String[0]),
                options.get(MongodbConfig.RETRY_MAX),
                options.get(MongodbConfig.RETRY_INTERVAL),
                options.get(MongodbConfig.TRANSACTION),
                options.get(MongodbConfig.SCHEMA_SAVE_MODE),
                options.get(MongodbConfig.DATA_SAVE_MODE));
    }
}
