package org.apache.seatunnel.connectors.selectdb.sink;

import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.selectdb.catalog.SelectDBCatalog;
import org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig;

public class SelectDBSaveModeHandler extends DefaultSaveModeHandler {

    private final SelectDBConfig selectDBConfig;

    SelectDBSaveModeHandler(
            SelectDBConfig selectDBConfig, Catalog catalog, CatalogTable catalogTable) {
        super(
                selectDBConfig.getSchemaSaveMode(),
                selectDBConfig.getDataSaveMode(),
                catalog,
                catalogTable,
                selectDBConfig.getCustomSql());
        this.selectDBConfig = selectDBConfig;
    }

    protected void createTable() {
        autoCreateTable(selectDBConfig.getSaveModeCreateTemplate());
    }

    private void autoCreateTable(String template) {
        String database = tablePath.getDatabaseName();
        String tableName = tablePath.getTableName();
        SelectDBCatalog catalog = (SelectDBCatalog) this.catalog;
        if (!catalog.databaseExists(database)) {
            catalog.createDatabase(TablePath.of(database, ""), true);
        }
        if (!catalog.tableExists(TablePath.of(database, tableName))) {
            catalog.createTable(
                    SelectDBSaveModeUtil.fillingCreateSql(
                            template, database, tableName, catalogTable.getTableSchema()));
        }
    }
}
