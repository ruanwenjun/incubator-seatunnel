package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;

import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.SOURCE_ALREADY_HAS_DATA;

public class JdbcSaveModeHandler {

    private final JdbcSinkConfig jdbcSinkConfig;

    private final ReadonlyConfig config;

    private final DataSaveMode saveMode;

    private final CatalogTable catalogTable;

    private final Catalog catalog;

    JdbcSaveModeHandler(JdbcSinkConfig jdbcSinkConfig,ReadonlyConfig config,DataSaveMode saveMode,CatalogTable catalogTable,Catalog catalog){
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.config = config;
        this.saveMode = saveMode;
        this.catalogTable = catalogTable;
        this.catalog = catalog;
    }

    public void doHandleSaveMode() {
        String fieldIde = config.get(JdbcOptions.FIELD_IDE);
        AbstractJdbcCatalog jdbcCatalog = (AbstractJdbcCatalog) catalog;
        TablePath tablePath =
                TablePath.of(
                        jdbcSinkConfig.getDatabase()
                                + "."
                                + CatalogUtils.quoteTableIdentifier(
                                jdbcSinkConfig.getTable(), fieldIde));
        switch (saveMode) {
            case DROP_SCHEMA:
                dropSchema(fieldIde, tablePath);
                break;
            case KEEP_SCHEMA_DROP_DATA:
                keepSchemaDropData(tablePath);
                break;
            case KEEP_SCHEMA_AND_DATA:
                keepSchemaAndData(tablePath);
                break;
            case CUSTOM_PROCESSING:
                customProcessing(jdbcCatalog, tablePath);
                break;
            case ERROR_WHEN_EXISTS:
                errorWhenExists(jdbcCatalog, tablePath);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported save mode: " + saveMode);
        }
    }

    private void errorWhenExists(AbstractJdbcCatalog jdbcCatalog, TablePath tablePath) {
        if (!catalog.tableExists(tablePath)) {
            catalog.createTable(tablePath, catalogTable, true);
        }
        if (jdbcCatalog.isExistsData(tablePath)) {
            throw new JdbcConnectorException(
                    SOURCE_ALREADY_HAS_DATA, "The target data source already has data");
        }
    }

    private void customProcessing(AbstractJdbcCatalog jdbcCatalog, TablePath tablePath) {
        if (!catalog.tableExists(tablePath)) {
            catalog.createTable(tablePath, catalogTable, true);
        }
        String customSql = config.get(JdbcOptions.CUSTOM_SQL);
        jdbcCatalog.executeSql(customSql);
    }

    private void keepSchemaAndData(TablePath tablePath) {
        if (!catalog.tableExists(tablePath)) {
            catalog.createTable(tablePath, catalogTable, true);
        }
    }

    private void keepSchemaDropData(TablePath tablePath) {
        if (catalog.tableExists(tablePath)) {
            catalog.truncateTable(tablePath, true);
        } else {
            catalog.createTable(tablePath, catalogTable, true);
        }
    }

    private void dropSchema(String fieldIde, TablePath tablePath) {
        if (!catalog.databaseExists(jdbcSinkConfig.getDatabase())) {
            catalog.createDatabase(tablePath, true);
        }
        if (catalog.tableExists(tablePath)) {
            catalog.dropTable(tablePath, true);
        }
        catalogTable.getOptions().put("fieldIde", fieldIde);
        if (!catalog.tableExists(tablePath)) {
            catalog.createTable(tablePath, catalogTable, true);
        }
    }
}
