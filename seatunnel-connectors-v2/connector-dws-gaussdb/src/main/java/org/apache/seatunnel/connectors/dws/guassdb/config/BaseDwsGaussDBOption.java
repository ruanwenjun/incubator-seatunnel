package org.apache.seatunnel.connectors.dws.guassdb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface BaseDwsGaussDBOption {

    Option<String> DATABASE =
            Options.key("database").stringType().noDefaultValue().withDescription("database");

    Option<String> DATABASE_SCHEMA =
            Options.key("database_schema")
                    .stringType()
                    .defaultValue("default")
                    .withDescription("database_schema");

    Option<String> TABLE =
            Options.key("table").stringType().noDefaultValue().withDescription("table");

    Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("jdbc url, eg:" + "jdbc:gaussdb://localhost:8000/postgres");

    Option<String> USER =
            Options.key("user").stringType().noDefaultValue().withDescription("jdbc user");

    Option<String> PASSWORD =
            Options.key("password").stringType().noDefaultValue().withDescription("jdbc password");

    Option<String> DRIVER =
            Options.key("driver")
                    .stringType()
                    .defaultValue("com.huawei.gauss200.jdbc.Driver")
                    .withDescription("driver");

    Option<Map<String, String>> PROPERTIES =
            Options.key("properties")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription("jdbc properties, eg: " + "{\n" + "ssl=true\n" + "}");

    Option<String> XA_DRIVER_CLASS_NAME =
            Options.key("xa_data_source_class_name")
                    .stringType()
                    .defaultValue("com.huawei.gauss200.jdbc.xa.PGXADataSource")
                    .withDescription("XA class name");

    Option<String> TABLE_PREFIX =
            Options.key("tablePrefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The table prefix name added when the table is automatically created");

    Option<String> TABLE_SUFFIX =
            Options.key("tableSuffix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The table suffix name added when the table is automatically created");

    Option<List<String>> PRIMARY_KEYS =
            Options.key("primary_keys")
                    .listType()
                    .defaultValue(new ArrayList<>())
                    .withDescription("primary keys");

    OptionRule getOptionRule();
}
