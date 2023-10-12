package org.apache.seatunnel.connectors.dolphindb.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.connectors.dolphindb.catalog.DolphinDBCatalog;
import org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig;
import org.apache.seatunnel.connectors.dolphindb.utils.DolphinDBSaveModeUtil;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.api.common.CommonOptions.FACTORY_ID;
import static org.apache.seatunnel.api.common.CommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.api.table.factory.FactoryUtil.discoverFactory;

@Slf4j
public class DolphinDBSaveModeHandler extends DefaultSaveModeHandler {

    private final CatalogTable catalogTable;
    private final ReadonlyConfig readonlyConfig;
    private final DolphinDBCatalog catalog;

    public DolphinDBSaveModeHandler(
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            CatalogTable catalogTable,
            ReadonlyConfig readonlyConfig) {
        super(
                schemaSaveMode,
                dataSaveMode,
                null,
                catalogTable,
                readonlyConfig.get(DolphinDBConfig.CUSTOM_SQL));
        this.readonlyConfig = readonlyConfig;
        this.catalogTable = catalogTable;
        this.catalog = createCatalog();
        catalog.open();
    }

    @Override
    protected boolean tableExists() {
        return catalog.tableExists(catalogTable.getTableId().toTablePath());
    }

    @Override
    protected void dropTable() {
        catalog.dropTable(catalogTable.getTableId().toTablePath(), true);
    }

    @Override
    protected void createTable() {
        // todo: it's not a good idea to auto create database and table in dolphindb, since it's
        // rely on many params
        String database = tablePath.getDatabaseName();
        String tableName = tablePath.getTableName();
        if (!catalog.databaseExists(database)) {
            catalog.createDatabase(TablePath.of(database, ""), true);
        }
        if (!catalog.tableExists(tablePath)) {
            String finalTemplate =
                    DolphinDBSaveModeUtil.fillingCreateSql(
                            readonlyConfig.get(DolphinDBConfig.SAVE_MODE_CREATE_TEMPLATE),
                            database,
                            tableName,
                            catalogTable.getTableSchema());
            catalog.executeScript(finalTemplate);
        }
    }

    @Override
    protected void truncateTable() {
        catalog.truncateTable(catalogTable.getTableId().toTablePath(), true);
    }

    @Override
    protected boolean dataExists() {
        return catalog.isExistsData(catalogTable.getTableId().toTablePath());
    }

    @Override
    protected void executeCustomSql() {
        catalog.executeScript(readonlyConfig.get(DolphinDBConfig.CUSTOM_SQL));
    }

    @Override
    public void close() throws Exception {
        try (DolphinDBCatalog closed = catalog) {}
    }

    private DolphinDBCatalog createCatalog() {
        Map<String, String> catalogOptions =
                readonlyConfig.getOptional(CatalogOptions.CATALOG_OPTIONS).orElse(new HashMap<>());
        String factoryId =
                catalogOptions.getOrDefault(FACTORY_ID.key(), readonlyConfig.get(PLUGIN_NAME));
        CatalogFactory catalogFactory =
                discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        CatalogFactory.class,
                        factoryId);
        if (catalogFactory == null) {
            return null;
        }
        // get catalog instance to operation database
        Catalog catalog =
                catalogFactory.createCatalog(catalogFactory.factoryIdentifier(), readonlyConfig);
        return (DolphinDBCatalog) catalog;
    }
}
