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

package org.apache.seatunnel.connectors.cdc.informix.source;

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.relational.connection.JdbcConnectionFactory;
import org.apache.seatunnel.connectors.cdc.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.informix.config.InformixSourceConfig;
import org.apache.seatunnel.connectors.cdc.informix.config.InformixSourceConfigFactory;
import org.apache.seatunnel.connectors.cdc.informix.source.eumerator.InformixChunkSplitter;
import org.apache.seatunnel.connectors.cdc.informix.source.reader.fetch.InformixSourceFetchTaskContext;
import org.apache.seatunnel.connectors.cdc.informix.source.reader.fetch.cdc.InformixCDCLogFetchTask;
import org.apache.seatunnel.connectors.cdc.informix.source.reader.fetch.snapshot.InformixSnapshotFetchTask;
import org.apache.seatunnel.connectors.cdc.informix.utils.InformixSchema;

import io.debezium.connector.informix.InformixConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.sql.SQLException;
import java.util.List;

public class InformixDialect implements JdbcDataSourceDialect {
    private final InformixSourceConfig sourceConfig;
    private transient InformixSchema informixSchema;

    public InformixDialect(InformixSourceConfigFactory configFactory) {
        this(configFactory.create(0));
    }

    public InformixDialect(InformixSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public String getName() {
        return "Informix";
    }

    @Override
    public InformixConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        InformixConnection connection =
                new InformixConnection(
                        sourceConfig.getDbzConfiguration(),
                        new JdbcConnectionFactory(sourceConfig, getPooledDataSourceFactory()));
        try {
            return connection.connect();
        } catch (Exception e) {
            throw new SeaTunnelException(e);
        }
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        try (InformixConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return jdbcConnection.isCaseSensitive();
        } catch (SQLException e) {
            throw new SeaTunnelException(
                    "Error reading Dameng system config: " + e.getMessage(), e);
        }
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new InformixChunkSplitter(sourceConfig, this);
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        InformixSourceConfig informixSourceConfig = (InformixSourceConfig) sourceConfig;
        try (InformixConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return jdbcConnection.listTables(informixSourceConfig.getTableFilters());
        } catch (SQLException e) {
            throw new SeaTunnelException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new InformixPooledDataSourceFactory();
    }

    @Override
    public TableChanges.TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (informixSchema == null) {
            synchronized (this) {
                if (informixSchema == null) {
                    informixSchema = new InformixSchema();
                }
            }
        }
        return informixSchema.getTableSchema(jdbc, tableId);
    }

    @Override
    public JdbcSourceFetchTaskContext createFetchTaskContext(
            SourceSplitBase sourceSplitBase, JdbcSourceConfig taskSourceConfig) {

        return new InformixSourceFetchTaskContext(taskSourceConfig, this);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new InformixSnapshotFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            return new InformixCDCLogFetchTask(sourceSplitBase.asIncrementalSplit());
        }
    }
}
