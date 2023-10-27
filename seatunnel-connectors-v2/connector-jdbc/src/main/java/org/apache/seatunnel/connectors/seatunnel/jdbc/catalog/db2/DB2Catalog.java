package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.db2;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
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
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.db2.DB2TypeMapper;

import org.apache.commons.lang3.StringUtils;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.db2.DB2DataTypeConvertor.DB2_BINARY;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.db2.DB2DataTypeConvertor.DB2_BLOB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.db2.DB2DataTypeConvertor.DB2_VARBINARY;

@Slf4j
public class DB2Catalog extends AbstractJdbcCatalog {

    protected final Map<String, Connection> connectionMap;

    private static final String SELECT_COLUMNS_SQL =
            "SELECT NAME AS column_name,\n"
                    + "       TYPENAME AS type_name,\n"
                    + "       TYPENAME AS full_type_name,\n"
                    + "       LENGTH AS column_length,\n"
                    + "       SCALE AS column_scale,\n"
                    + "       REMARKS AS column_comment,\n"
                    + "       DEFAULT  AS default_value,\n"
                    + "       NULLS AS is_nullable\n"
                    + "FROM SYSIBM.SYSCOLUMNS WHERE TBCREATOR = '%s' AND  TBNAME = '%s'";

    public DB2Catalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
        this.connectionMap = new ConcurrentHashMap<>();
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
                    && listTables(tablePath.getDatabaseName())
                            .contains(tablePath.getSchemaAndTableName());
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

        String dbUrl = getUrlFromDatabaseName(defaultDatabase);
        if (StringUtils.isNotBlank(tablePath.getDatabaseName())) {
            dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        }
        Connection conn = getConnection(dbUrl);
        try {
            DatabaseMetaData metaData = conn.getMetaData();
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

            String sql =
                    String.format(
                            SELECT_COLUMNS_SQL,
                            tablePath.getSchemaName(),
                            tablePath.getTableName());
            try (PreparedStatement ps = conn.prepareStatement(sql);
                    ResultSet resultSet = ps.executeQuery()) {
                TableSchema.Builder builder = TableSchema.builder();

                // add column
                while (resultSet.next()) {
                    buildColumn(resultSet, builder);
                }

                // add primary key
                primaryKey.ifPresent(builder::primaryKey);
                // add constraint key
                constraintKeys.forEach(builder::constraintKey);
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
            }

        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    private Map<String, String> buildConnectorOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>(8);
        options.put("connector", "jdbc");
        options.put("url", baseUrl + tablePath.getDatabaseName());
        options.put("table-name", tablePath.getFullName());
        options.put("username", username);
        options.put("password", pwd);
        return options;
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
        String sql =
                String.format(
                        "DROP TABLE IF EXISTS %s.%s ",
                        tablePath.getSchemaName(), "\"" + tablePath.getTableName() + "\"");
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
                String.format(
                        "TRUNCATE TABLE %s.%s immediate ",
                        tablePath.getSchemaName(), "\"" + tablePath.getTableName() + "\"");
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

    protected Optional<PrimaryKey> getPrimaryKey(
            DatabaseMetaData metaData, String database, String schema, String table)
            throws SQLException {
        return Optional.of(
                PrimaryKey.of(
                        getPrimaryKeyName(schema, table), getPrimaryKeyFieldList(schema, table)));
    }

    private List<String> getPrimaryKeyFieldList(String schema, String table) {
        String getPrimaryKeyFieldSql =
                String.format(
                        "SELECT COLNAME FROM SYSCAT.KEYCOLUSE WHERE TABSCHEMA = '%s' AND TABNAME = '%s';",
                        schema, table);
        Connection connection = defaultConnection;
        List<String> primaryKeyColNameList = new ArrayList<>();
        try (Statement ps = connection.createStatement()) {
            ResultSet resultSet = ps.executeQuery(getPrimaryKeyFieldSql);
            while (resultSet.next()) {
                String primaryKeyColName = resultSet.getString("COLNAME");
                primaryKeyColNameList.add(primaryKeyColName);
            }
            return primaryKeyColNameList;
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed getPrimaryKeyFieldList table %s", table), e);
        }
    }

    private String getPrimaryKeyName(String schema, String table) {
        String getPrimaryKeyNameSql =
                String.format(
                        "SELECT INDNAME FROM SYSCAT.INDEXES WHERE UNIQUERULE = 'P' AND TABSCHEMA  = '%s' AND TABNAME = '%s' ;",
                        schema, table);
        Connection connection = defaultConnection;
        try (Statement ps = connection.createStatement()) {
            ResultSet resultSet = ps.executeQuery(getPrimaryKeyNameSql);
            while (resultSet.next()) {
                String primaryKeyColName = resultSet.getString("INDNAME");
                if (StringUtils.isNotEmpty(primaryKeyColName)) {
                    return primaryKeyColName;
                }
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed getPrimaryKeyName table %s", table), e);
        }
        return null;
    }

    @Override
    public String getCountSql(TablePath tablePath) {
        return String.format(
                "select count(*) from %s.%s;",
                tablePath.getSchemaName(), "\"" + tablePath.getTableName() + "\"");
    }

    private String getUrlFromDatabaseName(String databaseName) {
        String url = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        return url + databaseName + suffix;
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

    private void buildColumn(ResultSet resultSet, TableSchema.Builder builder) throws SQLException {
        String columnName = resultSet.getString("column_name");
        String typeName = resultSet.getString("type_name").trim();
        String fullTypeName = resultSet.getString("full_type_name").trim();
        long columnLength = resultSet.getLong("column_length");
        long columnScale = resultSet.getLong("column_scale");
        String columnComment = resultSet.getString("column_comment");
        Object defaultValue = resultSet.getObject("default_value");
        boolean isNullable = resultSet.getString("is_nullable").equals("Y");

        SeaTunnelDataType<?> type = fromJdbcType(typeName, columnLength, columnScale);
        long bitLen = 0;
        switch (typeName) {
            case DB2_BLOB:
            case DB2_BINARY:
            case DB2_VARBINARY:
                bitLen = columnLength;
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
                        fullTypeName,
                        false,
                        false,
                        bitLen,
                        null,
                        columnLength);
        builder.column(physicalColumn);
    }

    private SeaTunnelDataType<?> fromJdbcType(String typeName, long precision, long scale) {
        Map<String, Object> dataTypeProperties = new HashMap<>();
        dataTypeProperties.put(DB2DataTypeConvertor.PRECISION, precision);
        dataTypeProperties.put(DB2DataTypeConvertor.SCALE, scale);
        return new DB2DataTypeConvertor().toSeaTunnelType(typeName, dataTypeProperties);
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new DB2TypeMapper());
    }
}
