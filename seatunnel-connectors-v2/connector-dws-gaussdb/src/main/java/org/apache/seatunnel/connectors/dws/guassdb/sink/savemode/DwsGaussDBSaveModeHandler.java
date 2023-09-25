package org.apache.seatunnel.connectors.dws.guassdb.sink.savemode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.dws.guassdb.catalog.DwsGaussDBCatalog;
import org.apache.seatunnel.connectors.dws.guassdb.sink.sql.DwsGaussSqlGenerator;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import static org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption.CUSTOM_SQL;

@Slf4j
public class DwsGaussDBSaveModeHandler {

    private final ReadonlyConfig readonlyConfig;

    private final CatalogTable catalogTable;
    private final DwsGaussSqlGenerator dwsGaussSqlGenerator;

    public DwsGaussDBSaveModeHandler(
            ReadonlyConfig readonlyConfig,
            CatalogTable catalogTable,
            DwsGaussSqlGenerator dwsGaussSqlGenerator) {
        this.readonlyConfig = readonlyConfig;
        this.catalogTable = catalogTable;
        this.dwsGaussSqlGenerator = dwsGaussSqlGenerator;
    }

    //    public void handleSaveMode(DataSaveMode saveMode) throws SQLException {
    //        try (DwsGaussDBCatalog dwsGaussDBCatalog =
    //                new DwsGaussDBCatalogFactory()
    //                        .createCatalog(catalogTable.getCatalogName(), readonlyConfig)) {
    //            // Create the temporary table if not exist
    //            if (readonlyConfig.get(DwsGaussDBSinkOption.WRITE_MODE)
    //                    == DwsGaussDBSinkOption.WriteMode.USING_TEMPORARY_TABLE) {
    //
    // dwsGaussDBCatalog.executeUpdateSql(dwsGaussSqlGenerator.getDropTemporaryTableSql());
    //                log.info(
    //                        "Drop temporary table: {} success",
    //                        dwsGaussSqlGenerator.getTemporaryTableName());
    //                dwsGaussDBCatalog.executeUpdateSql(
    //                        dwsGaussSqlGenerator.getCreateTemporaryTableSql());
    //                log.info(
    //                        "Create temporary table: {} success",
    //                        dwsGaussSqlGenerator.getTemporaryTableName());
    //            }
    //            switch (saveMode) {
    //                case DROP_SCHEMA:
    //                    dropSchema(dwsGaussDBCatalog);
    //                    break;
    //                case KEEP_SCHEMA_DROP_DATA:
    //                    keepSchemaDropData(dwsGaussDBCatalog);
    //                    break;
    //                case KEEP_SCHEMA_AND_DATA:
    //                    keepSchemaAndData(dwsGaussDBCatalog);
    //                    break;
    //                case CUSTOM_PROCESSING:
    //                    customProcessing(dwsGaussDBCatalog);
    //                    break;
    //                case ERROR_WHEN_EXISTS:
    //                    errorWhenExists(dwsGaussDBCatalog);
    //                    break;
    //                default:
    //                    throw new UnsupportedOperationException("Unsupported save mode: " +
    // saveMode);
    //            }
    //        }
    //    }

    private void dropSchema(DwsGaussDBCatalog dwsGaussDBCatalog) {
        dwsGaussDBCatalog.executeUpdateSql(dwsGaussSqlGenerator.getDropTargetTableSql());
        log.info("Drop table: {} success", dwsGaussSqlGenerator.getTargetTableName());
        dwsGaussDBCatalog.executeUpdateSql(dwsGaussSqlGenerator.getCreateTargetTableSql());
        log.info(
                "Create table: {} success using: {}",
                dwsGaussSqlGenerator.getTargetTableName(),
                dwsGaussSqlGenerator.getCreateTargetTableSql());
    }

    private void keepSchemaDropData(DwsGaussDBCatalog dwsGaussDBCatalog) {
        dwsGaussDBCatalog.executeUpdateSql(dwsGaussSqlGenerator.getDeleteTargetTableSql());
        log.info("Delete data in table: {}", dwsGaussSqlGenerator.getTargetTableName());
    }

    private void keepSchemaAndData(DwsGaussDBCatalog dwsGaussDBCatalog) {
        // We use IF NOT EXISTS
        dwsGaussDBCatalog.executeUpdateSql(dwsGaussSqlGenerator.getCreateTargetTableSql());
        log.info(
                "Create table: {} success using: {}",
                dwsGaussSqlGenerator.getTargetTableName(),
                dwsGaussSqlGenerator.getCreateTargetTableSql());
    }

    private void customProcessing(DwsGaussDBCatalog dwsGaussDBCatalog) {
        String customSql = readonlyConfig.get(CUSTOM_SQL);
        if (StringUtils.isEmpty(customSql)) {
            throw new IllegalArgumentException("The custom_sql is empty");
        }
        dwsGaussDBCatalog.executeSql(customSql);
        log.info("Execute custom sql success: {}", customSql);
    }

    private void errorWhenExists(DwsGaussDBCatalog dwsGaussDBCatalog) {
        // If the table exist, then will not create again
        if (!dwsGaussDBCatalog.tableExists(catalogTable.getTableId().toTablePath())) {
            // todo: use createTable
            dwsGaussDBCatalog.executeUpdateSql(dwsGaussSqlGenerator.getCreateTargetTableSql());
            log.info(
                    "Create table: {} success using: {}",
                    dwsGaussSqlGenerator.getTargetTableName(),
                    dwsGaussSqlGenerator.getCreateTargetTableSql());
        }
        if (dwsGaussDBCatalog.queryDataCount(dwsGaussSqlGenerator.getQuertTargetTableDataCountSql())
                > 0) {
            throw new IllegalStateException(
                    "The target table: "
                            + dwsGaussSqlGenerator.getTargetTableName()
                            + " already has data");
        }
    }
}
