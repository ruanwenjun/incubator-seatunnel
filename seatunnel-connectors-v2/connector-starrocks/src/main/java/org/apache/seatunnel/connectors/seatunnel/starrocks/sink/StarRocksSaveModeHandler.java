package org.apache.seatunnel.connectors.seatunnel.starrocks.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.starrocks.catalog.StarRocksCatalog;
import org.apache.seatunnel.connectors.seatunnel.starrocks.catalog.util.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import lombok.AllArgsConstructor;

import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.SOURCE_ALREADY_HAS_DATA;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksSinkOptions.CUSTOM_SQL;

@AllArgsConstructor
public class StarRocksSaveModeHandler {

    private final SinkConfig sinkConfig;
    private final DataSaveMode saveMode;
    private final ReadonlyConfig readonlyConfig;
    private final CatalogTable catalogTable;
    private final Catalog catalog;

    public void doHandleSaveMode() {
        String fieldIde = readonlyConfig.get(StarRocksSinkOptions.FIELD_IDE);
        TablePath tablePath =
                TablePath.of(
                        sinkConfig.getDatabase()
                                + "."
                                + CatalogUtils.quoteTableIdentifier(
                                        sinkConfig.getTable(), fieldIde));
        if (!catalog.databaseExists(sinkConfig.getDatabase())) {
            catalog.createDatabase(tablePath, true);
        }
        switch (saveMode) {
            case DROP_SCHEMA:
                if (catalog.tableExists(tablePath)) {
                    catalog.dropTable(tablePath, true);
                }
                autoCreateTable(sinkConfig.getSaveModeCreateTemplate());
                break;
            case KEEP_SCHEMA_DROP_DATA:
                if (catalog.tableExists(tablePath)) {
                    catalog.truncateTable(tablePath, true);
                } else {
                    autoCreateTable(sinkConfig.getSaveModeCreateTemplate());
                }
                break;
            case KEEP_SCHEMA_AND_DATA:
                if (!catalog.tableExists(tablePath)) {
                    autoCreateTable(sinkConfig.getSaveModeCreateTemplate());
                }
                break;
            case CUSTOM_PROCESSING:
                String sql = readonlyConfig.get(CUSTOM_SQL);
                ((StarRocksCatalog) catalog).executeSql(sql);
                break;
            case ERROR_WHEN_EXISTS:
                if (catalog.tableExists(tablePath)) {
                    if (((StarRocksCatalog) catalog).isExistsData(tablePath.getTableName())) {
                        throw new StarRocksConnectorException(
                                SOURCE_ALREADY_HAS_DATA, "The target data source already has data");
                    }
                }
                break;
        }
    }

    private void autoCreateTable(String template) {
        StarRocksCatalog starRocksCatalog =
                new StarRocksCatalog(
                        "StarRocks",
                        sinkConfig.getUsername(),
                        sinkConfig.getPassword(),
                        sinkConfig.getJdbcUrl());
        if (!starRocksCatalog.databaseExists(sinkConfig.getDatabase())) {
            starRocksCatalog.createDatabase(TablePath.of(sinkConfig.getDatabase(), ""), true);
        }
        if (!starRocksCatalog.tableExists(
                TablePath.of(sinkConfig.getDatabase(), sinkConfig.getTable()))) {
            starRocksCatalog.createTable(
                    StarRocksSaveModeUtil.fillingCreateSql(
                            template,
                            sinkConfig.getDatabase(),
                            sinkConfig.getTable(),
                            catalogTable.getTableSchema()));
        }
    }
}
