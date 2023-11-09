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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source;

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.config.OracleAgentSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source.eumerator.OracleChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source.reader.fetch.OracleAgentSourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source.reader.fetch.incremental.OracleAgentIncrementalFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source.reader.fetch.snapshot.OracleAgentSnapshotFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.utils.OracleConnectionUtils;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class OracleAgentDialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;
    private final OracleAgentSourceConfig sourceConfig;
    private transient OracleSchema oracleSchema;

    public OracleAgentDialect(OracleAgentSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public String getName() {
        return "OracleAgent-CDC";
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            OracleConnection oracleConnection = (OracleConnection) jdbcConnection;
            return oracleConnection.getOracleVersion().getMajor() == 11;
        } catch (SQLException e) {
            throw new SeaTunnelException("Error reading oracle variables: " + e.getMessage(), e);
        }
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        return OracleConnectionUtils.createOracleConnection(sourceConfig.getDbzConfiguration());
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new OracleChunkSplitter(sourceConfig, this);
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new OraclePooledDataSourceFactory();
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        long startTime = System.currentTimeMillis();

        OracleAgentSourceConfig oracleSourceConfig = (OracleAgentSourceConfig) sourceConfig;
        String database = oracleSourceConfig.getDbzConnectorConfig().getDatabaseName();

        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            List<TableId> tableIds =
                    OracleConnectionUtils.listTables(
                            jdbcConnection, database, oracleSourceConfig.getTableFilters());
            log.debug(
                    "CatalogCatalog discoverDataCollections for: {} success cost {}/ms",
                    database,
                    System.currentTimeMillis() - startTime);
            return tableIds;
        } catch (SQLException e) {
            throw new SeaTunnelException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        long startTime = System.currentTimeMillis();
        if (oracleSchema == null) {
            oracleSchema = new OracleSchema();
        }
        TableChange tableSchema = oracleSchema.getTableSchema(jdbc, tableId);
        log.debug(
                "CatalogCatalog queryTableSchema for: {} success cost {}/ms",
                tableId,
                System.currentTimeMillis() - startTime);
        return tableSchema;
    }

    @Override
    public OracleAgentSourceFetchTaskContext createFetchTaskContext(
            SourceSplitBase sourceSplitBase, JdbcSourceConfig taskSourceConfig) {
        List<TableChanges.TableChange> engineHistory = new ArrayList<>();
        try (OracleConnection connection =
                OracleConnectionUtils.createOracleConnection(
                        taskSourceConfig.getDbzConnectorConfig().getJdbcConfig())) {
            // TODO: support save table schema
            if (sourceSplitBase instanceof SnapshotSplit) {
                SnapshotSplit snapshotSplit = (SnapshotSplit) sourceSplitBase;
                engineHistory.add(queryTableSchema(connection, snapshotSplit.getTableId()));
            } else {
                IncrementalSplit incrementalSplit = (IncrementalSplit) sourceSplitBase;
                for (TableId tableId : incrementalSplit.getTableIds()) {
                    engineHistory.add(queryTableSchema(connection, tableId));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Query engine history error", e);
        }
        return new OracleAgentSourceFetchTaskContext(taskSourceConfig, this, engineHistory);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new OracleAgentSnapshotFetchTask(
                    sourceConfig, sourceSplitBase.asSnapshotSplit());
        } else {
            return new OracleAgentIncrementalFetchTask(
                    sourceConfig, sourceSplitBase.asIncrementalSplit());
        }
    }
}
