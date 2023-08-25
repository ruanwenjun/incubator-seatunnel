package org.apache.seatunnel.connectors.dolphindb.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.ADDRESS;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.DATABASE;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.PASSWORD;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.TABLE;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.USER;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.USE_SSL;

@AutoService(Factory.class)
public class DolphinDBCatalogFactory implements CatalogFactory {
    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        return new DolphinDBCatalog(
                catalogName,
                options.get(ADDRESS),
                options.get(USER),
                options.get(PASSWORD),
                options.get(DATABASE),
                options.get(TABLE),
                options.get(USE_SSL));
    }

    @Override
    public String factoryIdentifier() {
        return DolphinDBConfig.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(ADDRESS, USER, PASSWORD, DATABASE, TABLE, USE_SSL)
                .build();
    }
}
