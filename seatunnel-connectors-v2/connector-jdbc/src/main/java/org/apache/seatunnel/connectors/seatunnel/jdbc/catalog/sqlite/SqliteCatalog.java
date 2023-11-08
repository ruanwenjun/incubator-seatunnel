package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlite;

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
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MysqlDataTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlite.SqliteTypeMapper;

import com.mysql.cj.util.StringUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class SqliteCatalog extends AbstractJdbcCatalog {

    protected static final Set<String> SYS_DATABASES = new HashSet<>(4);

    protected final Map<String, Connection> connectionMap;

    public SqliteCatalog(
            String catalogName, String username, String pwd, JdbcUrlUtil.UrlInfo urlInfo) {
        // because sqlite no need username
        super(catalogName, "username", pwd, urlInfo, null);
        this.connectionMap = new ConcurrentHashMap<>();
    }

    public Connection getConnection(String url) {
        if (connectionMap.containsKey(url)) {
            return connectionMap.get(url);
        }
        try {
            Connection connection = DriverManager.getConnection(url);
            connectionMap.put(url, connection);
            return connection;
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed connecting to %s via JDBC.", url), e);
        }
    }

    @Override
    public void close() throws CatalogException {
        for (Map.Entry<String, Connection> entry : connectionMap.entrySet()) {
            try {
                entry.getValue().close();
            } catch (SQLException e) {
                throw new CatalogException(
                        String.format("Failed to close %s via JDBC.", entry.getKey()), e);
            }
        }
        super.close();
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {

        return true;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return new ArrayList<>();
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.catalogName, databaseName);
        }

        // sqlite nonsupport update databaseName
        Connection connection = getConnection(defaultUrl);
        try (PreparedStatement ps =
                connection.prepareStatement(
                        "select name from sqlite_master where type = \"table\";")) {

            ResultSet rs = ps.executeQuery();

            List<String> tables = new ArrayList<>();

            while (rs.next()) {
                tables.add(rs.getString(1));
            }

            return tables;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            return listTables(tablePath.getDatabaseName()).contains(tablePath.getTableName());
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

        Connection conn = getConnection(defaultUrl);
        try {
            DatabaseMetaData metaData = conn.getMetaData();

            Optional<PrimaryKey> primaryKey =
                    getPrimaryKey(metaData, tablePath.getDatabaseName(), tablePath.getTableName());
            List<ConstraintKey> constraintKeys =
                    getConstraintKeys(
                            metaData, tablePath.getDatabaseName(), tablePath.getTableName());
            try (ResultSet resultSet =
                    metaData.getColumns(
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName(),
                            null)) {

                TableSchema.Builder builder = TableSchema.builder();
                while (resultSet.next()) {
                    buildTable(resultSet, builder);
                }
                // add primary key
                primaryKey.ifPresent(builder::primaryKey);
                // add constraint key
                constraintKeys.forEach(builder::constraintKey);
                TableIdentifier tableIdentifier =
                        TableIdentifier.of(
                                catalogName, tablePath.getDatabaseName(), tablePath.getTableName());
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

    private void buildTable(ResultSet resultSet, TableSchema.Builder builder) throws SQLException {

        //        String columnName = resultSet.getString("COLUMN_NAME");
        //        String fullTypeName = resultSet.getString("TYPE_NAME");
        //        long columnLength = resultSet.getLong("COLUMN_SIZE");
        //        long columnScale = resultSet.getLong("DECIMAL_DIGITS");
        //        String columnComment = resultSet.getString("REMARKS");
        //        Object defaultValue = resultSet.getObject("COLUMN_DEF");
        //        boolean isNullable = resultSet.getInt("NULLABLE") == 1;

        String columnName = resultSet.getString("COLUMN_NAME");
        String fullTypeName = resultSet.getString("TYPE_NAME");
        long columnLength = resultSet.getLong("COLUMN_SIZE");
        long columnScale = resultSet.getLong("DECIMAL_DIGITS");
        String columnComment = resultSet.getString("REMARKS");
        Object defaultValue = resultSet.getObject("COLUMN_DEF");
        boolean isNullable = resultSet.getInt("NULLABLE") == 1;

        String dataType = JDBCType.valueOf(resultSet.getInt("DATA_TYPE")).getName();
        SeaTunnelDataType<?> type = fromJdbcType(dataType, columnLength, columnScale);

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
                        0L,
                        null,
                        columnLength);
        builder.column(physicalColumn);
    }

    public static Map<String, Object> getColumnsDefaultValue(TablePath tablePath, Connection conn) {
        StringBuilder queryBuf = new StringBuilder("SHOW FULL COLUMNS FROM ");
        queryBuf.append(StringUtils.quoteIdentifier(tablePath.getTableName(), "`", false));
        queryBuf.append(" FROM ");
        queryBuf.append(StringUtils.quoteIdentifier(tablePath.getDatabaseName(), "`", false));
        try (PreparedStatement ps2 = conn.prepareStatement(queryBuf.toString())) {
            ResultSet rs = ps2.executeQuery();
            Map<String, Object> result = new HashMap<>();
            while (rs.next()) {
                String field = rs.getString("Field");
                Object defaultValue = rs.getObject("Default");
                result.put(field, defaultValue);
            }
            return result;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed getting table(%s) columns default value",
                            tablePath.getFullName()),
                    e);
        }
    }

    // todo: If the origin source is mysql, we can directly use create table like to create the
    @Override
    protected boolean createTableInternal(TablePath tablePath, CatalogTable table)
            throws CatalogException {
        //
        //        String createTableSql =
        //                MysqlCreateTableSqlBuilder.builder(tablePath,
        // table).build(table.getCatalogName());
        //        createTableSql =
        //                CatalogUtils.getFieldIde(createTableSql,
        // table.getOptions().get("fieldIde"));
        //        Connection connection = getConnection(defaultUrl);
        //        log.info("create table sql: {}", createTableSql);
        //        try (PreparedStatement ps = connection.prepareStatement(createTableSql)) {
        //            ps.execute();
        //        } catch (Exception e) {
        //            throw new CatalogException(
        //                    String.format("Failed creating table %s", tablePath.getFullName()),
        // e);
        //        }
        return true;
    }

    @Override
    protected boolean dropTableInternal(TablePath tablePath) throws CatalogException {
        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        Connection connection = getConnection(dbUrl);
        try (PreparedStatement ps =
                connection.prepareStatement(
                        String.format("DROP TABLE IF EXISTS `%s`;", tablePath.getTableName()))) {
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
        Connection connection = getConnection(dbUrl);
        // sqlite nonsupport truncate
        try (PreparedStatement ps =
                connection.prepareStatement(
                        String.format("DELETE FROM `%s`;", tablePath.getTableName()))) {
            return ps.execute();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed truncating table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    protected boolean createDatabaseInternal(String databaseName) throws CatalogException {
        return true;
    }

    @Override
    protected boolean dropDatabaseInternal(String databaseName) throws CatalogException {
        return true;
    }

    public String getCountSql(TablePath tablePath) {
        return String.format("select count(*) from `%s`;", tablePath.getTableName());
    }

    private SeaTunnelDataType<?> fromJdbcType(String typeName, long precision, long scale) {
        Map<String, Object> dataTypeProperties = new HashMap<>();
        dataTypeProperties.put(MysqlDataTypeConvertor.PRECISION, precision);
        dataTypeProperties.put(MysqlDataTypeConvertor.SCALE, scale);
        return new SqliteDataTypeConvertor().toSeaTunnelType(typeName, dataTypeProperties);
    }

    @SuppressWarnings("MagicNumber")
    private Map<String, String> buildConnectorOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>(8);
        options.put("connector", "jdbc");
        options.put("url", baseUrl + tablePath.getDatabaseName());
        options.put("table-name", tablePath.getFullName());
        options.put("username", username);
        options.put("password", pwd);
        return options;
    }

    private String getUrlFromDatabaseName(String databaseName) {
        String url = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        return url + databaseName + suffix;
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new SqliteTypeMapper());
    }
}
