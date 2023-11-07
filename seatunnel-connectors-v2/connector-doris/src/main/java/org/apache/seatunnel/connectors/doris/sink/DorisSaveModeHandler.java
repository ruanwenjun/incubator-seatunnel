package org.apache.seatunnel.connectors.doris.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.doris.catalog.DorisCatalog;
import org.apache.seatunnel.connectors.doris.config.DorisConfig;

public class DorisSaveModeHandler extends DefaultSaveModeHandler {

    private final ReadonlyConfig readonlyConfig;

    DorisSaveModeHandler(
            ReadonlyConfig readonlyConfig, Catalog catalog, CatalogTable catalogTable) {
        super(
                readonlyConfig.get(DorisConfig.SCHEMA_SAVE_MODE),
                readonlyConfig.get(DorisConfig.DATA_SAVE_MODE),
                catalog,
                catalogTable,
                readonlyConfig.get(DorisConfig.CUSTOM_SQL));
        this.readonlyConfig = readonlyConfig;
    }

    protected void createTable() {
        autoCreateTable(readonlyConfig.get(DorisConfig.SAVE_MODE_CREATE_TEMPLATE));
    }

    private void autoCreateTable(String template) {
        String database = tablePath.getDatabaseName();
        String tableName = tablePath.getTableName();
        DorisCatalog catalog = (DorisCatalog) this.catalog;
        if (!catalog.databaseExists(database)) {
            catalog.createDatabase(TablePath.of(database, ""), true);
        }
        if (!catalog.tableExists(TablePath.of(database, tableName))) {
            catalog.createTable(
                    DorisSaveModeUtil.fillingCreateSql(
                            template, database, tableName, catalogTable.getTableSchema()));
        }
    }
}
