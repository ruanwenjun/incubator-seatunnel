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

package io.debezium.connector.informix;

import com.informix.jdbc.IfxDriver;
import com.informix.stream.api.IfmxStreamRecord;
import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Slf4j
@SuppressWarnings("MagicNumber")
public class InformixConnection extends JdbcConnection {
    private static final String URL_PATTERN =
            "jdbc:informix-sqli://${"
                    + JdbcConfiguration.HOSTNAME
                    + "}:${"
                    + JdbcConfiguration.PORT
                    + "}/${"
                    + JdbcConfiguration.DATABASE
                    + "}";
    private static final JdbcConnection.ConnectionFactory FACTORY =
            JdbcConnection.patternBasedFactory(
                    URL_PATTERN,
                    IfxDriver.class.getName(),
                    InformixConnection.class.getClassLoader(),
                    JdbcConfiguration.PORT.withDefault(
                            InformixConnectorConfig.PORT.defaultValueAsString()),
                    JdbcConfiguration.DATABASE);

    protected static final Set<String> SYS_DATABASES = new HashSet<>(9);

    static {
        SYS_DATABASES.add("sysmaster");
        SYS_DATABASES.add("sysutils");
        SYS_DATABASES.add("sysuser");
        SYS_DATABASES.add("sysadmin");
        SYS_DATABASES.add("syscdcv1");
    }

    private Configuration config;

    public InformixConnection(Configuration config) {
        this(config, FACTORY);
        this.config = config;
    }

    public InformixConnection(Configuration config, ConnectionFactory connectionFactory) {
        super(config, connectionFactory);
        this.config = config;
    }

    @Override
    public InformixConnection connect() throws SQLException {
        return (InformixConnection) super.connect();
    }

    public Lsn currentCheckpointLsn(InformixDatabaseSchema databaseSchema) {
        InformixCDCEngine cdcEngine = InformixCDCEngine.build(config);

        try {
            cdcEngine.init(databaseSchema);
            CompletableFuture<Long> sequenceId =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    long id = 0;
                                    while (id <= 0) {
                                        IfmxStreamRecord record =
                                                cdcEngine.getCdcEngine().getRecord();
                                        id = record.getSequenceId();
                                        if (id > 0) {
                                            cdcEngine.close();
                                        }
                                    }
                                    return id;
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });
            try (Connection connection = cdcEngine.getDs().getConnection()) {
                connection
                        .createStatement()
                        .execute(
                                "CREATE TABLE IF NOT EXISTS syscdcv1:informix.seatunnel_track (id int)");
                connection
                        .createStatement()
                        .execute("DELETE FROM syscdcv1:informix.seatunnel_track where id = 1");
                connection
                        .createStatement()
                        .execute("Insert Into syscdcv1:informix.seatunnel_track (id) values (1)");
            }
            return Lsn.valueOf(sequenceId.get());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Lsn currentCheckpointLsn() throws SQLException {
        return Lsn.valueOf(-1);
    }

    public Lsn earliestLsn() throws SQLException {
        String selectEarliestLsnSQL =
                "SELECT FIRST 1 curr_lsn "
                        + "FROM sysmaster:syssqltrace "
                        + "ORDER BY event_time ASC";
        JdbcConnection.ResultSetMapper<Lsn> mapper =
                rs -> {
                    rs.next();
                    return Lsn.valueOf(rs.getBigDecimal(2));
                };
        //        return queryAndMap(selectEarliestLsnSQL, mapper);
        return Lsn.valueOf(-1);
    }

    public List<String> listDatabases() throws SQLException {
        String sql = "select name from sysmaster:sysdatabases";
        JdbcConnection.ResultSetMapper<List<String>> mapper =
                rs -> {
                    List<String> databases = new ArrayList<>();
                    while (rs.next()) {
                        String databaseName = rs.getString(1).trim();
                        if (!SYS_DATABASES.contains(databaseName)) {
                            databases.add(databaseName);
                        }
                    }

                    return databases;
                };
        return queryAndMap(sql, mapper);
    }

    public List<TableId> listTables(RelationalTableFilters tableFilters) throws SQLException {
        return listDatabases().stream()
                .flatMap(
                        database -> {
                            String sql =
                                    "SELECT owner, tabname FROM "
                                            + database
                                            + ":informix.systables";
                            JdbcConnection.ResultSetMapper<List<TableId>> mapper =
                                    rs -> {
                                        List<TableId> capturedTableIds = new ArrayList<>();
                                        while (rs.next()) {
                                            String owner = rs.getString(1).trim();
                                            String table = rs.getString(2).trim();

                                            TableId tableId = new TableId(database, owner, table);
                                            if (tableFilters == null
                                                    || tableFilters
                                                            .dataCollectionFilter()
                                                            .isIncluded(tableId)) {
                                                capturedTableIds.add(tableId);
                                                log.info(
                                                        "\t including '{}' for further processing",
                                                        tableId);
                                            } else {
                                                log.info(
                                                        "\t '{}' is filtered out of capturing",
                                                        tableId);
                                            }
                                        }
                                        return capturedTableIds;
                                    };
                            try {
                                return queryAndMap(sql, mapper).stream();
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .collect(Collectors.toList());
    }

    public boolean isCaseSensitive() throws SQLException {
        String sql = "SELECT env_value FROM sysmaster:sysenv WHERE env_name = 'DELIMIDENT'";
        JdbcConnection.ResultSetMapper<Boolean> mapper =
                rs -> rs.next() && !rs.getString(1).equalsIgnoreCase("Y");
        return queryAndMap(sql, mapper);
    }

    public InformixCDCEngine getCdcEngine() {
        return InformixCDCEngine.build(config);
    }
}
