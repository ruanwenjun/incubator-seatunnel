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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractJdbcCatalog implements Catalog {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcCatalog.class);

    protected final String catalogName;
    protected final String defaultDatabase;
    protected final String username;
    protected final String pwd;
    protected final String baseUrl;
    protected final String suffix;
    protected final String defaultUrl;

    protected final Optional<String> defaultSchema;

    protected Connection defaultConnection;

    public AbstractJdbcCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {

        checkArgument(StringUtils.isNotBlank(username));
        checkArgument(StringUtils.isNotBlank(urlInfo.getUrlWithoutDatabase()));
        this.catalogName = catalogName;
        this.defaultDatabase = urlInfo.getDefaultDatabase().orElse(null);
        this.username = username;
        this.pwd = pwd;
        this.baseUrl = urlInfo.getUrlWithoutDatabase();
        this.defaultUrl = urlInfo.getOrigin();
        this.suffix = urlInfo.getSuffix();
        this.defaultSchema = Optional.ofNullable(defaultSchema);
    }

    @Override
    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return pwd;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    @Override
    public void open() throws CatalogException {
        try {
            defaultConnection = DriverManager.getConnection(defaultUrl, username, pwd);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed connecting to %s via JDBC.", defaultUrl), e);
        }

        LOG.info("Catalog {} established connection to {}", catalogName, defaultUrl);
    }

    @Override
    public void close() throws CatalogException {
        if (defaultConnection == null) {
            return;
        }
        try {
            defaultConnection.close();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed to close %s via JDBC.", defaultUrl), e);
        }
        LOG.info("Catalog {} closing", catalogName);
    }

    public void executeSql(String sql) {
        Connection connection = defaultConnection;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            // Will there exist concurrent drop for one table?
            ps.execute();
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed executeSql error %s", sql), e);
        }
    }

    public boolean isExistsData(TablePath tablePath) {
        Connection connection = defaultConnection;
        String sql = getCountSql(tablePath);
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ResultSet resultSet = ps.executeQuery();
            if (resultSet == null) {
                return false;
            }
            resultSet.next();
            int count = 0;
            count = resultSet.getInt(1);
            return count > 0;
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed executeSql error %s", sql), e);
        }
    }

    public abstract String getCountSql(TablePath tablePath);

    protected Optional<PrimaryKey> getPrimaryKey(
            DatabaseMetaData metaData, String database, String table) throws SQLException {
        return getPrimaryKey(metaData, database, table, table);
    }

    protected Optional<PrimaryKey> getPrimaryKey(
            DatabaseMetaData metaData, String database, String schema, String table)
            throws SQLException {
        return CatalogUtils.getPrimaryKey(metaData, TablePath.of(database, schema, table));
    }

    protected List<ConstraintKey> getConstraintKeys(
            DatabaseMetaData metaData, String database, String table) throws SQLException {
        return getConstraintKeys(metaData, database, table, table);
    }

    protected List<ConstraintKey> getConstraintKeys(
            DatabaseMetaData metaData, String database, String schema, String table)
            throws SQLException {
        return CatalogUtils.getConstraintKeys(metaData, TablePath.of(database, schema, table));
    }

    protected Optional<String> getColumnDefaultValue(
            DatabaseMetaData metaData, String database, String schema, String table, String column)
            throws SQLException {
        try (ResultSet resultSet = metaData.getColumns(database, schema, table, column)) {
            while (resultSet.next()) {
                String defaultValue = resultSet.getString("COLUMN_DEF");
                return Optional.ofNullable(defaultValue);
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        checkArgument(StringUtils.isNotBlank(databaseName));

        return listDatabases().contains(databaseName);
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            return databaseExists(tablePath.getDatabaseName())
                    && listTables(tablePath.getDatabaseName()).contains(tablePath.getTableName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");

        if (tableExists(tablePath) && !ignoreIfExists) {
            throw new TableAlreadyExistException(catalogName, tablePath);
        }

        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
        }
        if (defaultSchema.isPresent()) {
            tablePath =
                    new TablePath(
                            tablePath.getDatabaseName(),
                            defaultSchema.get(),
                            tablePath.getTableName());
        }
        createTableInternal(tablePath, table);
    }

    protected abstract boolean createTableInternal(TablePath tablePath, CatalogTable table)
            throws CatalogException;

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        if (!dropTableInternal(tablePath) && !ignoreIfNotExists) {
            throw new TableNotExistException(catalogName, tablePath);
        }
    }

    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        if (!truncateTableInternal(tablePath) && !ignoreIfNotExists) {
            throw new TableNotExistException(catalogName, tablePath);
        }
    }

    protected abstract boolean dropTableInternal(TablePath tablePath) throws CatalogException;

    protected abstract boolean truncateTableInternal(TablePath tablePath) throws CatalogException;

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(tablePath.getDatabaseName(), "Database name cannot be null");

        if (databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseAlreadyExistException(catalogName, tablePath.getDatabaseName());
        }
        if (!createDatabaseInternal(tablePath.getDatabaseName()) && !ignoreIfExists) {
            throw new DatabaseAlreadyExistException(catalogName, tablePath.getDatabaseName());
        }
    }

    protected abstract boolean createDatabaseInternal(String databaseName);

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(tablePath.getDatabaseName(), "Database name cannot be null");

        if (!dropDatabaseInternal(tablePath.getDatabaseName()) && !ignoreIfNotExists) {
            throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
        }
    }

    protected abstract boolean dropDatabaseInternal(String databaseName) throws CatalogException;

    public CatalogTable getTable(String sqlQuery) throws SQLException {
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery);
    }
}
