package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.db2;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;

import org.apache.commons.lang3.StringUtils;

import lombok.SneakyThrows;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MysqlCreateTableSqlBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class DB2Catalog extends AbstractJdbcCatalog {

    public DB2Catalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @SneakyThrows
    @Override
    public List<String> listDatabases() throws CatalogException {
        return Collections.singletonList(
                defaultConnection.getMetaData().getCatalogs().getMetaData().getCatalogName(1));
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            return databaseExists(tablePath.getDatabaseName())
                    && listTables(tablePath.getDatabaseName()).contains(tablePath.getSchemaAndTableName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    @SneakyThrows
    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        List<String> tableNames = new ArrayList<>();
        String getTableSql =
                "SELECT TABSCHEMA , TABNAME FROM SYSCAT.TABLES WHERE TABSCHEMA NOT IN ('SYSCAT','SYSIBM','SYSIBMADM','SYSPUBLIC','SYSSTAT','SYSTOOLS');";
        Statement statement = defaultConnection.createStatement();
        final ResultSet resultSet = statement.executeQuery(getTableSql);
        while (resultSet.next()) {
            String schemaName = resultSet.getString("TABSCHEMA");
            String tableName = resultSet.getString("TABNAME");
            if (StringUtils.isNotBlank(tableName)) {
                tableNames.add(schemaName.trim() + "." + tableName);
            }
        }
        return tableNames;
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        return null;
    }

    @Override
    protected boolean createTableInternal(TablePath tablePath, CatalogTable table)
            throws CatalogException {
        String createTableSql = new DB2CreateTableSqlBuilder(table).build(tablePath);
        createTableSql =
                CatalogUtils.getFieldIde(createTableSql, table.getOptions().get("fieldIde"));
        Connection connection = defaultConnection;
        log.info("create table sql: {}", createTableSql);
        try (PreparedStatement ps = connection.prepareStatement(createTableSql)) {
            return ps.execute();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed creating table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    protected boolean dropTableInternal(TablePath tablePath) throws CatalogException {
        String sql = String.format("DROP TABLE IF EXISTS %s ", tablePath.getSchemaAndTableName());
        Connection connection = defaultConnection;
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
        String sql =
                String.format("TRUNCATE TABLE %s immediate ", tablePath.getSchemaAndTableName());
        Connection connection = defaultConnection;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            // Will there exist concurrent drop for one table?
            return ps.execute();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed truncating table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    protected boolean createDatabaseInternal(String databaseName) {
        return false;
    }

    @Override
    protected boolean dropDatabaseInternal(String databaseName) throws CatalogException {
        return false;
    }

    @Override
    public String getCountSql(TablePath tablePath) {
        return String.format("select count(*) from %s;", tablePath.getFullName());
    }
}
