package org.apache.seatunnel.connectors.dws.guassdb.catalog;

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.dws.guassdb.config.DwsGaussDBConfig;
import org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption;

import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

public class DwsGaussDBCatalogFactory implements CatalogFactory {

    @Override
    public DwsGaussDBCatalog createCatalog(String catalogName, ReadonlyConfig options) {
        String urlWithDatabase = options.get(DwsGaussDBSinkOption.URL);
        Preconditions.checkArgument(
                StringUtils.isNoneBlank(urlWithDatabase),
                "Miss config url! Please check your config.");
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(urlWithDatabase);
        Optional<String> defaultDatabase = urlInfo.getDefaultDatabase();
        if (!defaultDatabase.isPresent()) {
            throw new OptionValidationException(DwsGaussDBSinkOption.URL);
        }
        return new DwsGaussDBCatalog(
                catalogName,
                options.get(DwsGaussDBSinkOption.USER),
                options.get(DwsGaussDBSinkOption.PASSWORD),
                urlInfo,
                options.get(DwsGaussDBSinkOption.PROPERTIES),
                options.get(DwsGaussDBSinkOption.DATABASE_SCHEMA));
    }

    @Override
    public String factoryIdentifier() {
        return DwsGaussDBConfig.CONNECTOR_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return new DwsGaussDBCatalogOption().getOptionRule();
    }
}
