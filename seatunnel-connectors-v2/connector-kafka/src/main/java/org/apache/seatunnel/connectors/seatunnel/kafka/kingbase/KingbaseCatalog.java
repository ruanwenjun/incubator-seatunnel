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

package org.apache.seatunnel.connectors.seatunnel.kafka.kingbase;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class KingbaseCatalog implements Catalog {

    private static final Logger LOG = LoggerFactory.getLogger(KingbaseCatalog.class);

    private final String baseUrl;
    private final String defaultUrl;
    private final String suffix;
    private final String catalogName;
    private final String username;
    private final String pwd;

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

    public KingbaseCatalog(
            String catalogName, String username, String pwd, JdbcUrlUtil.UrlInfo urlInfo) {
        this.catalogName = catalogName;
        this.username = username;
        this.pwd = pwd;
        String baseUrl = urlInfo.getUrlWithoutDatabase();
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        this.defaultUrl = urlInfo.getOrigin();
        this.suffix = urlInfo.getSuffix();
    }

    @Override
    public void open() throws CatalogException {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            // test connection, fail early if we cannot connect to database
            conn.getCatalog();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed connecting to %s via JDBC.", defaultUrl), e);
        }

        LOG.info("Catalog {} established connection to {}", catalogName, defaultUrl);
    }

    @Override
    public void close() throws CatalogException {}

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return null;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return false;
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
        return listTables(tablePath.getDatabaseName()).contains(tablePath.getSchemaAndTableName());
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }

        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
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
                    String columnName = resultSet.getString("COLUMN_NAME");
                    int columnDisplaySize = resultSet.getInt("COLUMN_SIZE");
                    String defaultValue = resultSet.getString("COLUMN_DEF");
                    PhysicalColumn physicalColumn =
                            PhysicalColumn.of(
                                    columnName,
                                    KingbaseTypeConvertor.mapping(resultSet),
                                    columnDisplaySize,
                                    Boolean.parseBoolean(
                                            resultSet.getObject("IS_NULLABLE").toString()),
                                    defaultValue,
                                    resultSet.getString("REMARKS"));
                    builder.column(physicalColumn);
                }
            }
            primaryKey.ifPresent(builder::primaryKey);
            TableIdentifier tableIdentifier =
                    TableIdentifier.of(
                            catalogName,
                            tablePath.getDatabaseName().toUpperCase(),
                            tablePath.getSchemaName().toUpperCase(),
                            tablePath.getTableName().toUpperCase());
            return CatalogTable.of(
                    tableIdentifier,
                    builder.build(),
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    "");
        } catch (Exception e) {
            throw new CatalogException("get table fields failed", e);
        }
    }

    private Optional<PrimaryKey> getPrimaryKey(
            DatabaseMetaData metaData, String dbName, String schema, String tableName)
            throws SQLException {
        ResultSet primaryKeysInfo = metaData.getPrimaryKeys(dbName, schema, tableName);
        if (primaryKeysInfo.next()) {
            String column = primaryKeysInfo.getString("COLUMN_NAME");
            String name = primaryKeysInfo.getString("PK_NAME");
            return Optional.of(PrimaryKey.of(name, Collections.singletonList(column)));
        }
        return Optional.empty();
    }

    private String getUrlFromDatabaseName(String databaseName) {
        return baseUrl + databaseName + suffix;
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }
}
