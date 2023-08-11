package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;

import static org.apache.seatunnel.api.table.factory.FactoryUtil.discoverFactory;

import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
public class JdbcAutoCreateTableHandler {

    private final CatalogTable catalogTable;

    private final ReadonlyConfig config;

    private final JdbcSinkConfig jdbcSinkConfig;

    public void doAutoCreatTable() {
        if (catalogTable == null) {
            return;
        }
        Map<String, String> catalogOptions = config.get(CatalogOptions.CATALOG_OPTIONS);
        if (catalogOptions == null) {
            return;
        }
        FieldIdeEnum fieldIdeEnum = config.get(JdbcOptions.FIELD_IDE);
        String fieldIde =
                fieldIdeEnum == null ? FieldIdeEnum.ORIGINAL.getValue() : fieldIdeEnum.getValue();
        catalogTable.getOptions().put("fieldIde", fieldIde);
        TablePath tablePath =
                TablePath.of(
                        jdbcSinkConfig.getDatabase()
                                + "."
                                + CatalogUtils.quoteTableIdentifier(
                                catalogTable.getTableId().getTableName(), fieldIde));
        Catalog catalog = this.createCatalog();
        if (catalog == null){
            return;
        }
        // table is exist？
        if (!catalog.tableExists(tablePath)){
            catalog.createTable(tablePath,catalogTable,true);
        }
    }

    private Catalog createCatalog() {
        Map<String, String> catalogOptions = config.get(CatalogOptions.CATALOG_OPTIONS);
        String factoryId = catalogOptions.get(CommonOptions.FACTORY_ID.key());
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
                catalogFactory.createCatalog(
                        catalogFactory.factoryIdentifier(),
                        ReadonlyConfig.fromMap(new HashMap<>(catalogOptions)));
        return catalog;
    }
}