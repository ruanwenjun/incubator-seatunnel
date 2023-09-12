package org.apache.seatunnel.connectors.dws.guassdb.sink.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.connectors.dws.guassdb.config.BaseDwsGaussDBOption;

import com.google.common.collect.Lists;

import java.util.List;

public class DwsGaussDBSinkOption implements BaseDwsGaussDBOption {

    public static final Option<DataSaveMode> SAVE_MODE =
            Options.key("save_mode")
                    .enumType(DataSaveMode.class)
                    .defaultValue(DataSaveMode.KEEP_SCHEMA_AND_DATA)
                    .withDescription("save_mode");

    public static final Option<String> CUSTOM_SQL =
            Options.key("custom_sql").stringType().noDefaultValue().withDescription("custom_sql");

    public static final Option<WriteMode> WRITE_MODE =
            Options.key("write_node")
                    .enumType(WriteMode.class)
                    .defaultValue(WriteMode.APPEND_ONLY)
                    .withDescription("write_node");

    public static final Option<List<String>> PRIMARY_KEYS =
            Options.key("primary_keys")
                    .listType(String.class)
                    .defaultValue(Lists.newArrayList("id"))
                    .withDescription("primary_keys");

    public static final Option<FieldIdeEnum> FIELD_IDE =
            Options.key("field_ide")
                    .enumType(FieldIdeEnum.class)
                    .noDefaultValue()
                    .withDescription("Whether case conversion is required");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size").intType().defaultValue(8196).withDescription("batch_size");

    public enum WriteMode {
        APPEND_ONLY,
        // todo: Add UPSERT mode(Doesn't use temporary table)
        USING_TEMPORARY_TABLE,
    }

    public enum FieldIdeEnum {
        ORIGINAL("original"), // Original string form
        UPPERCASE("uppercase"), // Convert to uppercase
        LOWERCASE("lowercase"); // Convert to lowercase

        private final String value;

        FieldIdeEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    @Override
    public OptionRule getOptionRule() {
        return OptionRule.builder()
                .required(URL, DRIVER, SAVE_MODE)
                .optional(USER, PASSWORD, PROPERTIES, XA_DRIVER_CLASS_NAME, WRITE_MODE, BATCH_SIZE)
                .conditional(WRITE_MODE, WriteMode.USING_TEMPORARY_TABLE, PRIMARY_KEYS)
                .build();
    }
}
