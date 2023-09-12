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

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import lombok.extern.slf4j.Slf4j;

import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
@SuppressWarnings("MagicNumber")
public class DamengConnection extends JdbcConnection {

    private static final Field URL = Field.create("url", "Raw JDBC url");

    public DamengConnection(Configuration config) {
        this(config, resolveConnectionFactory(config));
    }

    public DamengConnection(Configuration config, ConnectionFactory connectionFactory) {
        super(config, connectionFactory);
    }

    public DamengConnection(Configuration config, Supplier<ClassLoader> classLoaderSupplier) {
        super(config, resolveConnectionFactory(config), classLoaderSupplier);
    }

    public DamengConnection(
            Configuration config,
            ConnectionFactory connectionFactory,
            Supplier<ClassLoader> classLoaderSupplier) {
        super(config, connectionFactory, classLoaderSupplier);
    }

    @Override
    public DamengConnection connect() throws SQLException {
        return (DamengConnection) super.connect();
    }

    public Scn currentCheckpointLsn() throws SQLException {
        String selectCurrentCheckpointLsnSQL = "SELECT FILE_LSN FROM V$RLOG";
        JdbcConnection.ResultSetMapper<Scn> mapper =
                rs -> {
                    rs.next();
                    return Scn.valueOf(rs.getBigDecimal(1));
                };
        return queryAndMap(selectCurrentCheckpointLsnSQL, mapper);
    }

    public Scn earliestLsn() throws SQLException {
        String selectEarliestLsnSQL =
                "SELECT NAME, FIRST_CHANGE#, NEXT_CHANGE#, SEQUENCE# "
                        + "FROM V$ARCHIVED_LOG "
                        + "WHERE NAME IS NOT NULL AND STATUS='A' "
                        + "ORDER BY SEQUENCE# ASC LIMIT 1";
        JdbcConnection.ResultSetMapper<Scn> mapper =
                rs -> {
                    rs.next();
                    return Scn.valueOf(rs.getBigDecimal(2));
                };
        return queryAndMap(selectEarliestLsnSQL, mapper);
    }

    public List<TableId> listTables(RelationalTableFilters tableFilters, String database)
            throws SQLException {
        String sql = "SELECT OWNER, TABLE_NAME FROM ALL_TABLES";
        JdbcConnection.ResultSetMapper<List<TableId>> mapper =
                rs -> {
                    List<TableId> capturedTableIds = new ArrayList<>();
                    while (rs.next()) {
                        String owner = rs.getString(1);
                        String table = rs.getString(2);

                        TableId tableId = new TableId(database, owner, table);
                        if (tableFilters == null
                                || tableFilters.dataCollectionFilter().isIncluded(tableId)) {
                            capturedTableIds.add(tableId);
                            log.info("\t including '{}' for further processing", tableId);
                        } else {
                            log.info("\t '{}' is filtered out of capturing", tableId);
                        }
                    }
                    return capturedTableIds;
                };

        return queryAndMap(sql, mapper);
    }

    @Override
    public void readSchema(
            Tables tables,
            String databaseCatalog,
            String schemaNamePattern,
            Tables.TableFilter tableFilter,
            Tables.ColumnNameFilter columnFilter,
            boolean removeTablesNotFoundInJdbc)
            throws SQLException {
        super.readSchema(
                tables,
                null,
                schemaNamePattern,
                tableFilter,
                columnFilter,
                removeTablesNotFoundInJdbc);

        Set<TableId> tableIds =
                tables.tableIds().stream()
                        .filter(x -> schemaNamePattern.equals(x.schema()))
                        .collect(Collectors.toSet());

        for (TableId tableId : tableIds) {
            // super.readSchema() populates ids without the catalog; hence we apply the filtering
            // only
            // here and if a table is included, overwrite it with a new id including the catalog
            TableId tableIdWithCatalog =
                    new TableId(databaseCatalog, tableId.schema(), tableId.table());

            if (tableFilter.isIncluded(tableIdWithCatalog)) {
                overrideDamengSpecificColumnTypes(tables, tableId, tableIdWithCatalog);
            }

            tables.removeTable(tableId);
        }
    }

    public boolean isCaseSensitive() throws SQLException {
        String sql = "SELECT SF_GET_CASE_SENSITIVE_FLAG()";
        JdbcConnection.ResultSetMapper<Boolean> mapper =
                rs -> {
                    rs.next();
                    return rs.getInt(1) == 1;
                };
        return queryAndMap(sql, mapper);
    }

    public String getTableMetadataDdl(TableId tableId) throws SQLException {
        return queryAndMap(
                "SELECT dbms_metadata.get_ddl('TABLE','"
                        + tableId.table()
                        + "','"
                        + tableId.schema()
                        + "') FROM DUAL",
                rs -> {
                    if (!rs.next()) {
                        throw new DebeziumException(
                                "Could not get DDL metadata for table: " + tableId);
                    }

                    Object res = rs.getObject(1);
                    return ((Clob) res).getSubString(1, (int) ((Clob) res).length());
                });
    }

    public Scn getMaxArchiveLogScn() throws SQLException {
        String query =
                "SELECT MAX(NEXT_CHANGE#) FROM V$ARCHIVED_LOG WHERE NAME IS NOT NULL AND ARCHIVED = 'YES' AND STATUS = 'A'";
        return queryAndMap(
                query,
                (rs) -> {
                    if (rs.next()) {
                        return Scn.valueOf(rs.getString(1));
                    }
                    throw new DebeziumException("Could not obtain maximum archive log scn.");
                });
    }

    public Scn getFirstScn(Scn firstChange, Scn nextChange) throws SQLException {
        String sql =
                "SELECT "
                        + "   NAME, FIRST_CHANGE#, NEXT_CHANGE#, SEQUENCE# "
                        + "FROM "
                        + "(SELECT * FROM v$archived_log "
                        + "WHERE FIRST_CHANGE# <= ? AND NEXT_CHANGE# >= ? "
                        + "ORDER BY FIRST_CHANGE# DESC"
                        + ") t "
                        + "WHERE ROWNUM <= 1";
        StatementPreparer preparer =
                statement -> {
                    statement.setBigDecimal(1, firstChange.bigDecimalValue());
                    statement.setBigDecimal(2, nextChange.bigDecimalValue());
                };
        ResultSetMapper<Scn> mapper =
                rs -> {
                    if (rs.next()) {
                        return Scn.valueOf(rs.getBigDecimal(2));
                    }
                    return Scn.valueOf(0);
                };
        return prepareQueryAndMap(sql, preparer, mapper);
    }

    private static String join(String[] strings, String around, String splitter) {
        return Arrays.stream(strings)
                .map(s -> around + s + around)
                .collect(Collectors.joining(splitter));
    }

    public DamengConnection readLogContent(
            Scn startScn, String[] schemas, String[] tables, ResultSetConsumer consumer)
            throws SQLException {
        String sql =
                "SELECT "
                        + "   SCN, START_SCN, COMMIT_SCN, "
                        + "   TIMESTAMP, START_TIMESTAMP, COMMIT_TIMESTAMP, "
                        + "   XID, ROLL_BACK, OPERATION, OPERATION_CODE, SEG_OWNER, "
                        + "   TABLE_NAME, SQL_REDO, SQL_UNDO, "
                        + "   SSN, CSF, STATUS "
                        + "FROM V$LOGMNR_CONTENTS "
                        + "WHERE OPERATION != 'SELECT_FOR_UPDATE' "
                        + "   AND SCN > ? "
                        + "   AND (OPERATION = 'COMMIT' "
                        + "       OR (OPERATION = 'ROLLBACK' AND PXID != '0000000000000000') "
                        + "       OR (SEG_OWNER IN (%s) AND TABLE_NAME IN (%s)))";
        sql = String.format(sql, join(schemas, "'", ","), join(tables, "'", ","));

        StatementPreparer preparer =
                statement -> {
                    statement.setFetchSize(500);
                    statement.setBigDecimal(1, startScn.bigDecimalValue());
                };
        prepareQuery(sql, preparer, consumer);
        return this;
    }

    private static ConnectionFactory resolveConnectionFactory(Configuration config) {
        return JdbcConnection.patternBasedFactory(config.getString(URL));
    }

    private void overrideDamengSpecificColumnTypes(
            Tables tables, TableId tableId, TableId tableIdWithCatalog) {
        TableEditor editor = tables.editTable(tableId);
        editor.tableId(tableIdWithCatalog);

        List<String> columnNames = new ArrayList<>(editor.columnNames());
        for (String columnName : columnNames) {
            Column column = editor.columnWithName(columnName);
            if (column.jdbcType() == Types.TIMESTAMP) {
                editor.addColumn(
                        column.edit()
                                .length(column.scale().orElse(Column.UNSET_INT_VALUE))
                                .scale(null)
                                .create());
            }
            // NUMBER columns without scale value have it set to -127 instead of null;
            // let's rectify that
            else if (column.jdbcType() == 2) {
                column.scale()
                        .filter(s -> s == -127)
                        .ifPresent(
                                s -> {
                                    editor.addColumn(column.edit().scale(null).create());
                                });
            }
        }
        tables.overwriteTable(editor.create());
    }
}
