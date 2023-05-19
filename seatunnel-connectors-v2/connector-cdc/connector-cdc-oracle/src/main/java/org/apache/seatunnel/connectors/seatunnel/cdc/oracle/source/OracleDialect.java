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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source;

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.config.OracleSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.config.OracleSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.eumerator.OracleChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.reader.fetch.OracleSourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.reader.fetch.logminer.OracleStreamFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.reader.fetch.scan.OracleSnapshotFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils.OracleConnectionUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils.OracleSchema;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

import java.sql.SQLException;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils.OracleConnectionUtils.createOracleConnection;

public class OracleDialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;
    private final OracleSourceConfigFactory configFactory;
    private final OracleSourceConfig sourceConfig;
    private transient OracleSchema oracleSchema;

    public OracleDialect(OracleSourceConfigFactory configFactory) {
        this.configFactory = configFactory;
        this.sourceConfig = configFactory.create(0);
    }

    @Override
    public String getName() {
        return "Oracle";
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
        return createOracleConnection(sourceConfig.getDbzConfiguration());
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
        OracleSourceConfig oracleSourceConfig = (OracleSourceConfig) sourceConfig;

        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return OracleConnectionUtils.listTables(
                    jdbcConnection, oracleSourceConfig.getTableFilters());
        } catch (SQLException e) {
            throw new SeaTunnelException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (oracleSchema == null) {
            oracleSchema = new OracleSchema();
        }
        return oracleSchema.getTableSchema(jdbc, tableId);
    }

    @Override
    public OracleSourceFetchTaskContext createFetchTaskContext(
            SourceSplitBase sourceSplitBase, JdbcSourceConfig taskSourceConfig) {
        return new OracleSourceFetchTaskContext(taskSourceConfig, this);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new OracleSnapshotFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            return new OracleStreamFetchTask(sourceSplitBase.asIncrementalSplit());
        }
    }
}
