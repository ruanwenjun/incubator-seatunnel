package org.apache.seatunnel.connectors.dolphindb.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
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
public class DolphinDBSaveModeHandler {

    private final CatalogTable catalogTable;
    private final ReadonlyConfig readonlyConfig;
    private final SeaTunnelRowType seaTunnelRowType;
    private final DataSaveMode dataSaveMode;

    public DolphinDBSaveModeHandler(
            CatalogTable catalogTable,
            ReadonlyConfig readonlyConfig,
            SeaTunnelRowType seaTunnelRowType,
            DataSaveMode dataSaveMode) {
        this.catalogTable = catalogTable;
        this.readonlyConfig = readonlyConfig;
        this.seaTunnelRowType = seaTunnelRowType;
        this.dataSaveMode = dataSaveMode;
    }

    public void handleSaveMode() {
        TablePath tablePath;
        if (readonlyConfig.get(DolphinDBConfig.DATABASE) != null
                && readonlyConfig.get(DolphinDBConfig.TABLE) != null) {
            tablePath =
                    TablePath.of(
                            readonlyConfig.get(DolphinDBConfig.DATABASE),
                            readonlyConfig.get(DolphinDBConfig.TABLE));
        } else {
            tablePath = catalogTable.getTableId().toTablePath();
        }

        /*try (DolphinDBCatalog catalog = createCatalog()) {
            catalog.open();
            switch (dataSaveMode) {
                case DROP_SCHEMA:
                    if (catalog.tableExists(tablePath)) {
                        catalog.dropTable(tablePath, true);
                    }
                    autoCreateTableByTemplate(
                            catalog, tablePath, readonlyConfig.get(SAVE_MODE_CREATE_TEMPLATE));
                    break;
                case KEEP_SCHEMA_DROP_DATA:
                    if (catalog.tableExists(tablePath)) {
                        catalog.truncateTable(tablePath, true);
                    } else {
                        autoCreateTableByTemplate(
                                catalog, tablePath, readonlyConfig.get(SAVE_MODE_CREATE_TEMPLATE));
                    }
                    break;
                case KEEP_SCHEMA_AND_DATA:
                    if (!catalog.tableExists(tablePath)) {
                        autoCreateTableByTemplate(
                                catalog, tablePath, readonlyConfig.get(SAVE_MODE_CREATE_TEMPLATE));
                    }
                    break;
                case CUSTOM_PROCESSING:
                    String sql = readonlyConfig.get(DolphinDBConfig.CUSTOM_SQL);
                    catalog.executeScript(sql);
                    if (!catalog.tableExists(tablePath)) {
                        autoCreateTableByTemplate(
                                catalog, tablePath, readonlyConfig.get(SAVE_MODE_CREATE_TEMPLATE));
                    }
                    break;
                case ERROR_WHEN_EXISTS:
                    if (catalog.tableExists(tablePath)) {
                        if (catalog.isExistsData(tablePath)) {
                            throw new DolphinDBConnectorException(
                                    SOURCE_ALREADY_HAS_DATA,
                                    "The target data source already has data");
                        }
                    } else {
                        autoCreateTableByTemplate(
                                catalog, tablePath, readonlyConfig.get(SAVE_MODE_CREATE_TEMPLATE));
                    }
                    break;
            }
        }*/
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

    private void autoCreateTableByTemplate(
            DolphinDBCatalog catalog, TablePath tablePath, String template) {
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
                            template, database, tableName, catalogTable.getTableSchema());
            catalog.executeScript(finalTemplate);
        }
    }
}
