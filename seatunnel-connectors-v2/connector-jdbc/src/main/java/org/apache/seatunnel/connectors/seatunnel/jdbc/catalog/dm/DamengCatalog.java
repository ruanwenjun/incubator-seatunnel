/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.dm;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbTypeMapper;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class DamengCatalog extends AbstractJdbcCatalog {
    private static final DamengDataTypeConvertor DATA_TYPE_CONVERTOR =
            new DamengDataTypeConvertor();
    private static final List<String> EXCLUDED_SCHEMAS =
            Collections.unmodifiableList(
                    Arrays.asList("SYS", "SYSDBA", "SYSSSO", "SYSAUDITOR", "CTISYS"));

    private static final String SELECT_COLUMNS_SQL =
            "SELECT COLUMNS.COLUMN_NAME, COLUMNS.DATA_TYPE, COLUMNS.DATA_LENGTH, COLUMNS.DATA_PRECISION, COLUMNS.DATA_SCALE "
                    + ", COLUMNS.NULLABLE, COLUMNS.DATA_DEFAULT, COMMENTS.COMMENTS ,"
                    + "CASE \n"
                    + "        WHEN COLUMNS.DATA_TYPE IN ('CHAR', 'CHARACTER', 'VARCHAR', 'VARCHAR2', 'VARBINARY', 'BINARY') THEN COLUMNS.DATA_TYPE || '(' || COLUMNS.DATA_LENGTH || ')'\n"
                    + "        WHEN COLUMNS.DATA_TYPE IN ('NUMERIC', 'DECIMAL', 'NUMBER') AND COLUMNS.DATA_PRECISION IS NOT NULL AND COLUMNS.DATA_SCALE IS NOT NULL AND COLUMNS.DATA_PRECISION != 0 AND COLUMNS.DATA_SCALE != 0 THEN COLUMNS.DATA_TYPE || '(' || COLUMNS.DATA_PRECISION || ', ' || COLUMNS.DATA_SCALE || ')'\n"
                    + "        ELSE COLUMNS.DATA_TYPE\n"
                    + "    END AS SOURCE_TYPE \n"
                    + "FROM ALL_TAB_COLUMNS COLUMNS "
                    + "LEFT JOIN ALL_COL_COMMENTS COMMENTS "
                    + "ON COLUMNS.OWNER = COMMENTS.SCHEMA_NAME "
                    + "AND COLUMNS.TABLE_NAME = COMMENTS.TABLE_NAME "
                    + "AND COLUMNS.COLUMN_NAME = COMMENTS.COLUMN_NAME "
                    + "WHERE COLUMNS.OWNER = ? "
                    + "AND COLUMNS.TABLE_NAME = ? "
                    + "ORDER BY COLUMNS.COLUMN_ID ASC";

    public DamengCatalog(
            String catalogName, String username, String pwd, JdbcUrlUtil.UrlInfo urlInfo) {
        super(catalogName, username, pwd, urlInfo, null);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try (PreparedStatement ps =
                defaultConnection.prepareStatement("SELECT NAME FROM v$database")) {

            List<String> databases = new ArrayList<>();
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String databaseName = rs.getString(1);
                databases.add(databaseName);
            }
            return databases;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", this.catalogName), e);
        }
    }

    @Override
    protected boolean createTableInternal(TablePath tablePath, CatalogTable table)
            throws CatalogException {
        String createTableSql = new DamengCreateTableSqlBuilder(table).build(tablePath);
        String[] createTableSqls = createTableSql.split(";");
        for (String sql : createTableSqls) {
            log.info("create table sql: {}", sql);
            try (PreparedStatement ps = defaultConnection.prepareStatement(sql)) {
                ps.execute();
            } catch (Exception e) {
                throw new CatalogException(
                        String.format(
                                "Failed creating table %s.%s",
                                tablePath.getSchemaName(), tablePath.getTableName()),
                        e);
            }
        }
        return true;
    }

    @Override
    protected boolean dropTableInternal(TablePath tablePath) throws CatalogException {
        Connection connection = defaultConnection;
        try (PreparedStatement ps =
                connection.prepareStatement(
                        String.format(
                                "DROP TABLE \"%s\".\"%s\"",
                                tablePath.getSchemaName(), tablePath.getTableName()))) {
            // Will there exist concurrent truncate for one table?
            return ps.execute();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed truncating table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    protected boolean truncateTableInternal(TablePath tablePath) throws CatalogException {
        Connection connection = defaultConnection;
        try (PreparedStatement ps =
                connection.prepareStatement(
                        String.format(
                                "TRUNCATE TABLE \"%s\".\"%s\"",
                                tablePath.getSchemaName(), tablePath.getTableName()))) {
            // Will there exist concurrent truncate for one table?
            return ps.execute();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed truncating table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    protected boolean createDatabaseInternal(String databaseName) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean dropDatabaseInternal(String databaseName) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    public String getCountSql(TablePath tablePath) {
        return String.format(
                "select count(*) from \"%s\".\"%s\"",
                tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            if (StringUtils.isNotBlank(tablePath.getDatabaseName())) {
                return databaseExists(tablePath.getDatabaseName())
                        && listTables(tablePath.getDatabaseName())
                                .contains(tablePath.getSchemaAndTableName());
            }
            return listTables().contains(tablePath.getSchemaAndTableName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    private List<String> listTables() {
        List<String> databases = listDatabases();
        return listTables(databases.get(0));
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.catalogName, databaseName);
        }

        try (PreparedStatement ps =
                        defaultConnection.prepareStatement(
                                "SELECT OWNER, TABLE_NAME FROM ALL_TABLES");
                ResultSet rs = ps.executeQuery()) {

            List<String> tables = new ArrayList<>();
            while (rs.next()) {
                if (EXCLUDED_SCHEMAS.contains(rs.getString(1))) {
                    continue;
                }
                tables.add(rs.getString(1) + "." + rs.getString(2));
            }

            return tables;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing table in catalog %s", catalogName), e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }

        try {
            DatabaseMetaData metaData = defaultConnection.getMetaData();
            Optional<PrimaryKey> primaryKey =
                    getPrimaryKey(
                            metaData,
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName());
            List<ConstraintKey> constraintKeys =
                    getConstraintKeys(
                            metaData,
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName());

            TableSchema.Builder builder = TableSchema.builder();
            primaryKey.ifPresent(builder::primaryKey);
            constraintKeys.forEach(builder::constraintKey);
            builder.columns(getColumns(tablePath));
            TableIdentifier tableIdentifier =
                    TableIdentifier.of(
                            catalogName,
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName());
            return CatalogTable.of(
                    tableIdentifier,
                    builder.build(),
                    buildConnectorOptions(tablePath),
                    Collections.emptyList(),
                    "",
                    catalogName);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    private List<Column> getColumns(TablePath tablePath) throws SQLException {
        List<Column> columns = new ArrayList<>();

        try (PreparedStatement ps = defaultConnection.prepareStatement(SELECT_COLUMNS_SQL)) {
            ps.setString(1, tablePath.getSchemaName());
            ps.setString(2, tablePath.getTableName());
            try (ResultSet resultSet = ps.executeQuery()) {
                while (resultSet.next()) {
                    String columnName = resultSet.getString("COLUMN_NAME");
                    String typeName = resultSet.getString("DATA_TYPE");
                    String sourceTypeName = resultSet.getString("SOURCE_TYPE");
                    long columnLength = resultSet.getLong("DATA_LENGTH");
                    long columnPrecision = resultSet.getLong("DATA_PRECISION");
                    long columnScale = resultSet.getLong("DATA_SCALE");
                    String columnComment = resultSet.getString("COMMENTS");
                    Object defaultValue = resultSet.getObject("DATA_DEFAULT");
                    boolean isNullable = resultSet.getString("NULLABLE").equals("Y");

                    SeaTunnelDataType<?> type =
                            fromJdbcType(typeName, columnPrecision, columnScale);
                    long bitLen = 0;
                    long longColumnLength = 0;
                    switch (typeName) {
                        case DamengDataTypeConvertor.DM_BIT:
                            bitLen = columnLength;
                            break;
                        case DamengDataTypeConvertor.DM_DECIMAL:
                        case DamengDataTypeConvertor.DM_TIMESTAMP:
                        case DamengDataTypeConvertor.DM_DATETIME:
                        case DamengDataTypeConvertor.DM_TIME:
                            columnLength = columnScale;
                            break;
                        case DamengDataTypeConvertor.DM_CHAR:
                        case DamengDataTypeConvertor.DM_CHARACTER:
                        case DamengDataTypeConvertor.DM_VARCHAR:
                        case DamengDataTypeConvertor.DM_VARCHAR2:
                        case DamengDataTypeConvertor.DM_LONGVARCHAR:
                        case DamengDataTypeConvertor.DM_CLOB:
                        case DamengDataTypeConvertor.DM_TEXT:
                        case DamengDataTypeConvertor.DM_LONG:
                        case DamengDataTypeConvertor.DM_BFILE:
                            longColumnLength = columnLength;
                            break;
                        case DamengDataTypeConvertor.DM_BINARY:
                        case DamengDataTypeConvertor.DM_VARBINARY:
                        case DamengDataTypeConvertor.DM_BLOB:
                        case DamengDataTypeConvertor.DM_IMAGE:
                        case DamengDataTypeConvertor.DM_LONGVARBINARY:
                            bitLen = columnLength * 8;
                            break;
                        default:
                            break;
                    }
                    PhysicalColumn physicalColumn =
                            PhysicalColumn.of(
                                    columnName,
                                    type,
                                    ((int) columnLength),
                                    isNullable,
                                    defaultValue,
                                    columnComment,
                                    sourceTypeName,
                                    false,
                                    false,
                                    bitLen,
                                    null,
                                    longColumnLength);
                    columns.add(physicalColumn);
                }
            }
        }
        return columns;
    }

    private SeaTunnelDataType<?> fromJdbcType(String typeName, long precision, long scale) {
        Map<String, Object> dataTypeProperties = new HashMap<>();
        dataTypeProperties.put(DamengDataTypeConvertor.PRECISION, precision);
        dataTypeProperties.put(DamengDataTypeConvertor.SCALE, scale);
        return DATA_TYPE_CONVERTOR.toSeaTunnelType(typeName, dataTypeProperties);
    }

    @SuppressWarnings("MagicNumber")
    private Map<String, String> buildConnectorOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>(8);
        options.put("connector", "jdbc");
        options.put("url", baseUrl);
        options.put("table-name", tablePath.getSchemaAndTableName());
        options.put("username", username);
        options.put("password", pwd);
        return options;
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new DmdbTypeMapper());
    }
}
