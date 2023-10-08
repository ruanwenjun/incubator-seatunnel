package org.apache.seatunnel.connectors.dws.guassdb.sink.savemode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.dws.guassdb.catalog.DwsGaussDBCatalog;
import org.apache.seatunnel.connectors.dws.guassdb.catalog.DwsGaussDBCatalogFactory;
import org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption;
import org.apache.seatunnel.connectors.dws.guassdb.sink.sql.DwsGaussSqlGenerator;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import static org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption.CUSTOM_SQL;

@Slf4j
public class DwsGaussDBSaveModeHandler extends DefaultSaveModeHandler {

    private final ReadonlyConfig readonlyConfig;

    private final CatalogTable catalogTable;
    private final DwsGaussSqlGenerator dwsGaussSqlGenerator;

    private final DwsGaussDBCatalog dwsGaussDBCatalog;

    public DwsGaussDBSaveModeHandler(
            ReadonlyConfig readonlyConfig,
            CatalogTable catalogTable,
            DwsGaussSqlGenerator dwsGaussSqlGenerator) {
        super(
                readonlyConfig.get(DwsGaussDBSinkOption.SCHEMA_SAVE_MODE),
                readonlyConfig.get(DwsGaussDBSinkOption.DATA_SAVE_MODE),
                null,
                catalogTable,
                readonlyConfig.get(DwsGaussDBSinkOption.CUSTOM_SQL));
        this.readonlyConfig = readonlyConfig;
        this.catalogTable = catalogTable;
        this.dwsGaussSqlGenerator = dwsGaussSqlGenerator;
        this.dwsGaussDBCatalog =
                new DwsGaussDBCatalogFactory()
                        .createCatalog(catalogTable.getCatalogName(), readonlyConfig);
    }

    @Override
    protected boolean tableExists() {
        return dwsGaussDBCatalog.tableExists(catalogTable.getTableId().toTablePath());
    }

    @Override
    protected void dropTable() {
        dwsGaussDBCatalog.executeUpdateSql(dwsGaussSqlGenerator.getDropTargetTableSql());
        log.info("Drop table: {} success", dwsGaussSqlGenerator.getTargetTableName());
        if (readonlyConfig.get(DwsGaussDBSinkOption.WRITE_MODE)
                == DwsGaussDBSinkOption.WriteMode.USING_TEMPORARY_TABLE) {
            dwsGaussDBCatalog.executeUpdateSql(dwsGaussSqlGenerator.getDropTemporaryTableSql());
            log.info("Drop temporary table: {} success", dwsGaussSqlGenerator.getTargetTableName());
        }
    }

    @Override
    protected void createTable() {
        // We use IF NOT EXISTS
        dwsGaussDBCatalog.executeUpdateSql(dwsGaussSqlGenerator.getCreateTargetTableSql());
        log.info(
                "Create table: {} success using: {}",
                dwsGaussSqlGenerator.getTargetTableName(),
                dwsGaussSqlGenerator.getCreateTargetTableSql());
        if (readonlyConfig.get(DwsGaussDBSinkOption.WRITE_MODE)
                == DwsGaussDBSinkOption.WriteMode.USING_TEMPORARY_TABLE) {
            dwsGaussDBCatalog.executeUpdateSql(dwsGaussSqlGenerator.getCreateTemporaryTableSql());
            log.info(
                    "Create temporary table: {} success using: {}",
                    dwsGaussSqlGenerator.getTemporaryTableName(),
                    dwsGaussSqlGenerator.getCreateTemporaryTableSql());
        }
    }

    @Override
    protected void truncateTable() {
        dwsGaussDBCatalog.executeUpdateSql(dwsGaussSqlGenerator.getDeleteTargetTableSql());
        log.info("Delete data in table: {}", dwsGaussSqlGenerator.getTargetTableName());

        if (readonlyConfig.get(DwsGaussDBSinkOption.WRITE_MODE)
                == DwsGaussDBSinkOption.WriteMode.USING_TEMPORARY_TABLE) {
            dwsGaussDBCatalog.executeUpdateSql(dwsGaussSqlGenerator.getDeleteTemporaryTableSql());
            log.info(
                    "Delete data in temporary table: {}",
                    dwsGaussSqlGenerator.getTemporaryTableName());
        }
    }

    @Override
    protected boolean dataExists() {
        return dwsGaussDBCatalog.queryDataCount(
                        dwsGaussSqlGenerator.getQuertTargetTableDataCountSql())
                > 0;
    }

    @Override
    protected void executeCustomSql() {
        String customSql = readonlyConfig.get(CUSTOM_SQL);
        if (StringUtils.isEmpty(customSql)) {
            throw new IllegalArgumentException("The custom_sql is empty");
        }
        dwsGaussDBCatalog.executeSql(customSql);
        log.info("Execute custom sql success: {}", customSql);
    }

    @Override
    public void close() throws Exception {
        try (DwsGaussDBCatalog closed = dwsGaussDBCatalog) {}
    }
}
