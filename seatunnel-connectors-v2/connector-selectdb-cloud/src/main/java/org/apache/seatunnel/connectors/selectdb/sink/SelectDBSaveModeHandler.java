package org.apache.seatunnel.connectors.selectdb.sink;

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.selectdb.catalog.SelectDBCatalog;
import org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig;
import org.apache.seatunnel.connectors.selectdb.exception.SelectDBConnectorException;

import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.SOURCE_ALREADY_HAS_DATA;

public class SelectDBSaveModeHandler {

    private final SelectDBConfig selectDBConfig;
    private final DataSaveMode saveMode;

    private final CatalogTable catalogTable;

    private final SelectDBCatalog catalog;
    private final TablePath tablePath;

    SelectDBSaveModeHandler(
            SelectDBConfig selectDBConfig,
            DataSaveMode saveMode,
            CatalogTable catalogTable,
            SelectDBCatalog catalog) {
        this.selectDBConfig = selectDBConfig;
        this.saveMode = saveMode;
        this.catalogTable = catalogTable;
        this.catalog = catalog;
        if (selectDBConfig.getTableIdentifier() != null) {
            tablePath = TablePath.of(selectDBConfig.getTableIdentifier());
        } else {
            tablePath = catalogTable.getTableId().toTablePath();
        }
    }

    public void doHandleSaveMode() {
        if (!catalog.databaseExists(tablePath.getDatabaseName())) {
            catalog.createDatabase(tablePath, true);
        }
        switch (saveMode) {
            case DROP_SCHEMA:
                if (catalog.tableExists(tablePath)) {
                    catalog.dropTable(tablePath, true);
                }
                autoCreateTable(selectDBConfig.getSaveModeCreateTemplate());
                break;
            case KEEP_SCHEMA_DROP_DATA:
                if (catalog.tableExists(tablePath)) {
                    catalog.truncateTable(tablePath, true);
                } else {
                    autoCreateTable(selectDBConfig.getSaveModeCreateTemplate());
                }
                break;
            case KEEP_SCHEMA_AND_DATA:
                if (!catalog.tableExists(tablePath)) {
                    autoCreateTable(selectDBConfig.getSaveModeCreateTemplate());
                }
                break;
            case CUSTOM_PROCESSING:
                String sql = selectDBConfig.getCustomSql();
                catalog.executeSql(sql);
                if (!catalog.tableExists(tablePath)) {
                    autoCreateTable(selectDBConfig.getSaveModeCreateTemplate());
                }
                break;
            case ERROR_WHEN_EXISTS:
                if (catalog.tableExists(tablePath)) {
                    if (catalog.isExistsData(tablePath.getFullName())) {
                        throw new SelectDBConnectorException(
                                SOURCE_ALREADY_HAS_DATA, "The target data source already has data");
                    }
                } else {
                    autoCreateTable(selectDBConfig.getSaveModeCreateTemplate());
                }
                break;
        }
    }

    private void autoCreateTable(String template) {
        String database = tablePath.getDatabaseName();
        String tableName = tablePath.getTableName();
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
