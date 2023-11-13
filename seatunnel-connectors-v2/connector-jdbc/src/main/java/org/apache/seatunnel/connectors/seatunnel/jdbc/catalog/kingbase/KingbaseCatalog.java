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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.kingbase;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
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
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.kingbase.KingBaseTypeMapper;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class KingbaseCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(KingbaseCatalog.class);

    public static final Set<String> KINGBASE_SYSTEM_DATABASES =
            Sets.newHashSet("TEMPLATE1", "TEMPLATE0", "TEMPLATE2", "SAMPLES", "SECURITY");

    public static final Set<String> KINGBASE_SYSTEM_SCHEMAS =
            Sets.newHashSet(
                    "information_schema",
                    "SYS_CATALOG",
                    "SYSAUDIT",
                    "SYS_HM",
                    "SYS_TOAST",
                    "SYS_TEMP_1",
                    "SYSLOGICAL",
                    "SYS_TOAST_TEMP_1");

    protected final Map<String, Connection> connectionMap;

    public KingbaseCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
        this.connectionMap = new ConcurrentHashMap<>();
    }

    @Override
    public String getCountSql(TablePath tablePath) {
        return String.format("select count(*) from %s;", tablePath.getFullName());
    }

    @Override
    protected boolean createTableInternal(TablePath tablePath, CatalogTable table)
            throws CatalogException {
        String createTableSql =
                new KingbaseCreateTableSqlBuilder(table)
                        .build(tablePath, table.getOptions().get("fieldIde"));
        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        Connection conn = getConnection(dbUrl);
        LOG.info("create table sql: {}", createTableSql);
        try (PreparedStatement ps = conn.prepareStatement(createTableSql)) {
            ps.execute();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed creating table %s", tablePath.getFullName()), e);
        }
        return true;
    }

    @Override
    protected boolean dropTableInternal(TablePath tablePath) throws CatalogException {
        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());

        String schemaName = tablePath.getSchemaName();
        String tableName = tablePath.getTableName();

        String sql = "DROP TABLE IF EXISTS \"" + schemaName + "\".\"" + tableName + "\"";
        Connection connection = getConnection(dbUrl);
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            // Will there exist concurrent drop for one table?
            return ps.execute();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed dropping table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    protected boolean truncateTableInternal(TablePath tablePath) throws CatalogException {
        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());

        String schemaName = tablePath.getSchemaName();
        String tableName = tablePath.getTableName();

        String sql = "TRUNCATE TABLE  \"" + schemaName + "\".\"" + tableName + "\"";
        Connection connection = getConnection(dbUrl);
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            // Will there exist concurrent drop for one table?
            return ps.execute();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed truncating table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    protected boolean createDatabaseInternal(String databaseName) throws CatalogException {
        String sql = "CREATE DATABASE \"" + databaseName + "\"";
        try (PreparedStatement ps = defaultConnection.prepareStatement(sql)) {
            return ps.execute();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed creating database %s in catalog %s",
                            databaseName, this.catalogName),
                    e);
        }
    }

    @Override
    protected boolean dropDatabaseInternal(String databaseName) throws CatalogException {
        String sql = "DROP DATABASE IF EXISTS \"" + databaseName + "\"";
        try (PreparedStatement ps = defaultConnection.prepareStatement(sql)) {
            return ps.execute();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed dropping database %s in catalog %s",
                            databaseName, this.catalogName),
                    e);
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> dbNames = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(defaultUrl, username, pwd);
                PreparedStatement statement =
                        connection.prepareStatement("select datname from sys_database;")) {
            ResultSet re = statement.executeQuery();
            while (re.next()) {
                String dbName = re.getString("datname");
                if (StringUtils.isNotBlank(dbName) && !KINGBASE_SYSTEM_DATABASES.contains(dbName)) {
                    dbNames.add(dbName);
                }
            }
            return dbNames;
        } catch (Exception e) {
            throw new CatalogException("get databases failed", e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        List<String> tableNames = new ArrayList<>();
        String query = "SELECT table_schema, table_name FROM information_schema.tables";
        String dbUrl = getUrlFromDatabaseName(databaseName);
        try (Connection connection = DriverManager.getConnection(dbUrl, username, pwd);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                String schemaName = resultSet.getString("table_schema");
                String tableName = resultSet.getString("table_name");
                if (StringUtils.isNotBlank(schemaName)
                        && !KINGBASE_SYSTEM_SCHEMAS.contains(schemaName)) {
                    tableNames.add(schemaName + "." + tableName);
                }
            }
            return tableNames;
        } catch (Exception e) {
            throw new CatalogException("get table names failed", e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            if (StringUtils.isNotBlank(tablePath.getDatabaseName())) {
                return databaseExists(tablePath.getDatabaseName())
                        && listTables(tablePath.getDatabaseName())
                                .contains(tablePath.getSchemaAndTableName());
            }

            return listTables(defaultDatabase).contains(tablePath.getSchemaAndTableName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }

        String dbUrl;
        if (StringUtils.isBlank(tablePath.getDatabaseName())) {
            dbUrl = getUrlFromDatabaseName(defaultDatabase);
        } else {
            dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        }
        TableSchema.Builder builder = TableSchema.builder();
        try (Connection connection = DriverManager.getConnection(dbUrl, username, pwd)) {
            DatabaseMetaData metaData = connection.getMetaData();
            Optional<PrimaryKey> primaryKey =
                    getPrimaryKey(
                            metaData,
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName());
            try (ResultSet resultSet =
                    metaData.getColumns(
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName(),
                            null)) {
                while (resultSet.next()) {
                    String pgType = resultSet.getString("TYPE_NAME").toUpperCase();
                    String columnName = resultSet.getString("COLUMN_NAME");
                    int columnDisplaySize = resultSet.getInt("COLUMN_SIZE");
                    String defaultValue = resultSet.getString("COLUMN_DEF");
                    int columnSize = resultSet.getInt("COLUMN_SIZE");
                    int decimalDigits = resultSet.getInt("DECIMAL_DIGITS");
                    int nullable = resultSet.getInt("NULLABLE");
                    String remarks = resultSet.getString("REMARKS");

                    PhysicalColumn physicalColumn =
                            PhysicalColumn.of(
                                    columnName,
                                    fromJdbcType(pgType, columnSize, decimalDigits),
                                    columnDisplaySize,
                                    nullable != ResultSetMetaData.columnNoNulls,
                                    defaultValue,
                                    remarks,
                                    pgType,
                                    false,
                                    false,
                                    null,
                                    null,
                                    null);
                    builder.column(physicalColumn);
                }
            }
            primaryKey.ifPresent(builder::primaryKey);
            TableIdentifier tableIdentifier =
                    TableIdentifier.of(
                            catalogName,
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName());
            return CatalogTable.of(
                    tableIdentifier, builder.build(), new HashMap<>(), new ArrayList<>(), "");
        } catch (Exception e) {
            throw new CatalogException("get table fields failed", e);
        }
    }

    public Connection getConnection(String url) {
        if (connectionMap.containsKey(url)) {
            return connectionMap.get(url);
        }
        try {
            Connection connection = DriverManager.getConnection(url, username, pwd);
            connectionMap.put(url, connection);
            return connection;
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed connecting to %s via JDBC.", url), e);
        }
    }

    private String getUrlFromDatabaseName(String databaseName) {
        String url = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        return url + databaseName + suffix;
    }

    private SeaTunnelDataType<?> fromJdbcType(String typeName, long precision, long scale) {
        Map<String, Object> dataTypeProperties = new HashMap<>();
        dataTypeProperties.put(KingBaseDataTypeConvertor.PRECISION, precision);
        dataTypeProperties.put(KingBaseDataTypeConvertor.SCALE, scale);
        return new KingBaseDataTypeConvertor().toSeaTunnelType(typeName, dataTypeProperties);
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new KingBaseTypeMapper());
    }
}
