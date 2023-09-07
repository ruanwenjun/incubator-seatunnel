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
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor;

import lombok.extern.slf4j.Slf4j;

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

import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_BFILE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_BLOB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_CHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_CLOB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_LONG;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_LONG_RAW;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_NCHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_NCLOB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_NVARCHAR2;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_RAW;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_ROWID;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_VARCHAR2;

@Slf4j
public class DamengCatalog extends AbstractJdbcCatalog {
    private static final DamengDataTypeConvertor DATA_TYPE_CONVERTOR =
            new DamengDataTypeConvertor();
    private static final List<String> EXCLUDED_SCHEMAS =
            Collections.unmodifiableList(
                    Arrays.asList("SYS", "SYSDBA", "SYSSSO", "SYSAUDITOR", "CTISYS"));

    private static final String SELECT_COLUMNS_SQL =
            "SELECT COLUMNS.COLUMN_NAME, COLUMNS.DATA_TYPE, COLUMNS.DATA_LENGTH, COLUMNS.DATA_PRECISION, COLUMNS.DATA_SCALE "
                    + ", COLUMNS.NULLABLE, COLUMNS.DATA_DEFAULT, COMMENTS.COMMENTS "
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
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean dropTableInternal(TablePath tablePath) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean truncateTableInternal(TablePath tablePath) throws CatalogException {
        throw new UnsupportedOperationException();
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
                "select count(*) from %s.%s", tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            return databaseExists(tablePath.getDatabaseName())
                    && listTables(tablePath.getDatabaseName())
                            .contains(tablePath.getSchemaAndTableName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
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
                    "");
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
                    long columnLength = resultSet.getLong("DATA_LENGTH");
                    long columnPrecision = resultSet.getLong("DATA_PRECISION");
                    long columnScale = resultSet.getLong("DATA_SCALE");
                    String columnComment = resultSet.getString("COMMENTS");
                    Object defaultValue = resultSet.getObject("DATA_DEFAULT");
                    boolean isNullable = resultSet.getString("NULLABLE").equals("Y");

                    SeaTunnelDataType<?> type =
                            fromJdbcType(typeName, columnPrecision, columnScale);
                    long bitLen = 0;
                    switch (typeName) {
                        case ORACLE_LONG:
                        case ORACLE_ROWID:
                        case ORACLE_NCLOB:
                        case ORACLE_CLOB:
                            columnLength = -1;
                            break;
                        case ORACLE_RAW:
                            bitLen = 2000 * 8;
                            break;
                        case ORACLE_BLOB:
                        case ORACLE_LONG_RAW:
                        case ORACLE_BFILE:
                            bitLen = -1;
                            break;
                        case ORACLE_CHAR:
                        case ORACLE_NCHAR:
                        case ORACLE_NVARCHAR2:
                        case ORACLE_VARCHAR2:
                        default:
                            break;
                    }

                    PhysicalColumn physicalColumn =
                            PhysicalColumn.of(
                                    columnName,
                                    type,
                                    0,
                                    isNullable,
                                    defaultValue,
                                    columnComment,
                                    typeName,
                                    false,
                                    false,
                                    bitLen,
                                    null,
                                    columnLength);
                    columns.add(physicalColumn);
                }
            }
        }
        return columns;
    }

    private SeaTunnelDataType<?> fromJdbcType(String typeName, long precision, long scale) {
        Map<String, Object> dataTypeProperties = new HashMap<>();
        dataTypeProperties.put(OracleDataTypeConvertor.PRECISION, precision);
        dataTypeProperties.put(OracleDataTypeConvertor.SCALE, scale);
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
}
