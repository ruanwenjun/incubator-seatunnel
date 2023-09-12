package org.apache.seatunnel.connectors.dolphindb.catalog;

import org.apache.seatunnel.api.configuration.util.ConfigUtil;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.connectors.dolphindb.datatyle.DolphinDBDataTypeConvertor;
import org.apache.seatunnel.connectors.dolphindb.exception.DolphinDBConnectorException;

import org.apache.commons.collections4.CollectionUtils;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicStringVector;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;
import com.xxdb.data.Vector;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.seatunnel.connectors.dolphindb.exception.DolphinDBErrorCode.CHECK_DATA_ERROR;
import static org.apache.seatunnel.connectors.dolphindb.exception.DolphinDBErrorCode.EXECUTE_CUSTOMER_SCRIPT_ERROR;

@Slf4j
public class DolphinDBCatalog implements Catalog {

    private final String catalogName;
    private final List<String> addresses;
    private final String user;
    private final String password;
    private final String databaseName;
    private final String tableName;
    private final boolean useSSL;

    private DBConnection dbConnection;

    public DolphinDBCatalog(
            String catalogName,
            List<String> addresses,
            String user,
            String password,
            String databaseName,
            String tableName,
            boolean useSSL) {
        if (catalogName == null) {
            log.warn("The catalogName is null");
        }
        this.catalogName = catalogName;
        this.addresses = checkNotNull(addresses);
        this.user = checkNotNull(user);
        this.password = checkNotNull(password);
        this.databaseName = checkNotNull(databaseName);
        this.tableName = tableName;
        this.useSSL = useSSL;
    }

    @Override
    public void open() throws CatalogException {
        dbConnection = new DBConnection(false, useSSL);
        String address = addresses.get(0);
        String host = address.substring(0, address.lastIndexOf(":"));
        int port = Integer.parseInt(address.substring(address.lastIndexOf(":") + 1));
        try {
            if (!dbConnection.connect(host, port, user, password)) {
                throw new CatalogException("Cannot connect to DolphinDB cluster");
            }
        } catch (IOException e) {
            throw new CatalogException("Cannot connect to DolphinDB cluster", e);
        }
    }

    @Override
    public void close() throws CatalogException {
        if (dbConnection != null) {
            dbConnection.close();
        }
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return databaseName;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return listDatabases().contains(databaseName);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            BasicStringVector run =
                    (BasicStringVector) dbConnection.run("getClusterDFSDatabases()");
            return Arrays.stream(run.getdataArray()).collect(Collectors.toList());
        } catch (IOException e) {
            throw new CatalogException("DolphinDB listDatabases failed", e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        try {
            BasicStringVector run =
                    (BasicStringVector)
                            dbConnection.run("getDFSTablesByDatabase(\"" + databaseName + "\")");
            return Arrays.stream(run.getdataArray())
                    .map(tableFullName -> tableFullName.substring(databaseName.length() + 1))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new CatalogException("DolphinDB listTables in " + databaseName + " failed", e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        List<String> allTables = listTables(tablePath.getDatabaseName());
        if (allTables.contains(tablePath.getTableName())) {
            return true;
        }
        return false;
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        try {
            BasicTable basicTable =
                    (BasicTable)
                            dbConnection.run(
                                    String.format(
                                            "loadTable(\"%s\",\"%s\").schema().colDefs",
                                            tablePath.getDatabaseName(), tablePath.getTableName()));
            int columns = basicTable.columns();
            TableSchema.Builder builder = TableSchema.builder();
            DolphinDBDataTypeConvertor dolphinDBDataTypeConvertor =
                    new DolphinDBDataTypeConvertor();
            for (int i = 0; i < columns; i++) {
                Vector column = basicTable.getColumn(i);
                String columnName = basicTable.getColumnName(i);
                Entity.DATA_TYPE dataType = column.getDataType();
                PhysicalColumn physicalColumn =
                        PhysicalColumn.of(
                                columnName,
                                dolphinDBDataTypeConvertor.toSeaTunnelType(dataType.name()),
                                null,
                                true,
                                null,
                                null);
                builder.column(physicalColumn);
            }
            return CatalogTable.of(
                    TableIdentifier.of(
                            catalogName, tablePath.getDatabaseName(), tablePath.getTableName()),
                    builder.build(),
                    buildTableOptions(tablePath),
                    Collections.emptyList(),
                    "");
        } catch (IOException ioException) {
            throw new CatalogException(
                    "DolphinDB getTable for: " + tablePath + " failed", ioException);
        }
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
        }
        if (!createTableInternal(tablePath, table) && !ignoreIfExists) {
            throw new TableAlreadyExistException(catalogName, tablePath);
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
        }
        if (!tableExists(tablePath)) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(catalogName, tablePath);
            }
        } else {
            dropTableInternal(tablePath);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException(
                "DolphinDBCatalog does not support create database");
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("DolphinDBCatalog does not support drop database");
    }

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException("table not exist", tablePath);
        }
        try {
            dbConnection.run(
                    String.format(
                            "delete from loadTable(\"%s\", \"%s\");",
                            tablePath.getDatabaseName(), tablePath.getTableName()));
            // The truncate only support after 2.0.0.8
            // dbConnection.run(String.format("truncate(\"%s\", `%s)", tablePath.getDatabaseName(),
            // tablePath.getTableName()));
        } catch (IOException ioException) {
            throw new CatalogException(
                    "DolphinDB truncateTable for: " + tablePath + " failed", ioException);
        }
    }

    public void dropTableInternal(TablePath tablePath)
            throws DatabaseNotExistException, CatalogException {
        StringBuilder sb = new StringBuilder();
        sb.append("db=database(\"" + tablePath.getDatabaseName() + "\");\n");
        sb.append("dropTable(db, \"" + tablePath.getTableName() + "\");\n");
        try {
            Entity run = dbConnection.run(sb.toString());
            System.out.println(run);
        } catch (IOException ioException) {
            throw new CatalogException(
                    "DolphinDB dropTableInternal for: " + tablePath + " failed", ioException);
        }
    }

    public void executeScript(String script) {
        try {
            Entity run = dbConnection.run(script);
            if (run == null) {
                throw new DolphinDBConnectorException(EXECUTE_CUSTOMER_SCRIPT_ERROR, script);
            }
        } catch (IOException ioException) {
            throw new DolphinDBConnectorException(
                    EXECUTE_CUSTOMER_SCRIPT_ERROR, script, ioException);
        }
    }

    private boolean createTableInternal(TablePath tablePath, CatalogTable table) {
        String createTableSql = "create table \"%s\".\"%s\"(\n" + " %s\n" + " )\n";
        List<String> columns = new ArrayList<>();
        DolphinDBDataTypeConvertor dolphinDBDataTypeConvertor = new DolphinDBDataTypeConvertor();
        table.getTableSchema()
                .getColumns()
                .forEach(
                        column -> {
                            columns.add(
                                    column.getName()
                                            + " "
                                            + dolphinDBDataTypeConvertor
                                                    .toConnectorType(column.getDataType(), null)
                                                    .name());
                        });
        createTableSql =
                String.format(
                        createTableSql,
                        tablePath.getDatabaseName(),
                        tablePath.getTableName(),
                        String.join(",\n", columns));
        if (CollectionUtils.isNotEmpty(table.getPartitionKeys())) {
            createTableSql =
                    createTableSql
                            + " partitioned by "
                            + String.join(",", table.getPartitionKeys())
                            + " ;\n";
        }
        try {
            dbConnection.run(createTableSql);
            return true;
        } catch (IOException ioException) {
            throw new CatalogException(
                    "DolphinDB createTableInternal for: " + tablePath + " failed", ioException);
        }
    }

    private Map<String, String> buildTableOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>();
        // todo: do we need to add connect info here?
        options.put("connector", "dolphindb");
        options.put("config", ConfigUtil.convertToJsonString(tablePath));
        return options;
    }

    public boolean isExistsData(TablePath tablePath) {
        try {
            BasicTable basicTable =
                    (BasicTable)
                            dbConnection.run(
                                    String.format(
                                            "loadTable(\"%s\",\"%s\").schema().colDefs",
                                            tablePath.getDatabaseName(), tablePath.getTableName()));
            return basicTable.rows() > 0;
        } catch (IOException e) {
            throw new DolphinDBConnectorException(CHECK_DATA_ERROR, e);
        }
    }
}
