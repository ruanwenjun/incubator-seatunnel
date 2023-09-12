package org.apache.seatunnel.connectors.dws.guassdb.catalog;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.connectors.dws.guassdb.config.BaseDwsGaussDBOption;

public class DwsGaussDBCatalogOption implements BaseDwsGaussDBOption {

    // todo: move to catalog
    @Override
    public OptionRule getOptionRule() {
        return OptionRule.builder()
                .required(URL, DRIVER, DATABASE, SCHEMA)
                .optional(USER, PASSWORD, PROPERTIES)
                .build();
    }
}
