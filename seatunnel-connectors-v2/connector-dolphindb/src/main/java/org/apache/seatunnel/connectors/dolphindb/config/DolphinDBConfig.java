package org.apache.seatunnel.connectors.dolphindb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;
import lombok.experimental.UtilityClass;

import java.util.List;

import static org.apache.seatunnel.api.sink.DataSaveMode.KEEP_SCHEMA_AND_DATA;

@UtilityClass
public class DolphinDBConfig {

    public static final String PLUGIN_NAME = "DolphinDB";

    public static final Option<List<String>> ADDRESS =
            Options.key("address")
                    .listType(String.class)
                    .noDefaultValue()
                    .withDescription("DolphinDB host, eg:" + "[\"localhost:8848\"]");

    public static final Option<String> USER =
            Options.key("user").stringType().noDefaultValue().withDescription("username");

    public static final Option<String> PASSWORD =
            Options.key("password").stringType().noDefaultValue().withDescription("password");

    public static final Option<String> DATABASE =
            Options.key("database").stringType().noDefaultValue().withDescription("database path");

    public static final Option<String> TABLE =
            Options.key("table").stringType().noDefaultValue().withDescription("table name");
    public static final Option<Boolean> USE_SSL =
            Options.key("useSSL").booleanType().defaultValue(false).withDescription("use ssl");
    public static final Option<Integer> BATCH_SIZE =
            Options.key("batchSize").intType().defaultValue(1024).withDescription("batch size");

    public static final Option<Integer> THROTTLE =
            Options.key("throttle")
                    .intType()
                    .defaultValue(10)
                    .withDescription("max flush interval");

    public static final Option<String> PARTITION_COLUMN =
            Options.key("partitionColumn")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("partition column");

    public static final Option<MultithreadedTableWriter.Mode> WRITE_MODE =
            Options.key("writeMode")
                    .enumType(MultithreadedTableWriter.Mode.class)
                    .defaultValue(MultithreadedTableWriter.Mode.M_Upsert)
                    .withDescription("write mode");

    public static final Option<List<String>> KEY_COL_NAMES =
            Options.key("keyColNames")
                    .listType(String.class)
                    .noDefaultValue()
                    .withDescription("mode option, eg: [false, \"`id\"]");

    public static final Option<List<Integer>> COMPRESS_TYPE =
            Options.key("compressType")
                    .listType(Integer.class)
                    .noDefaultValue()
                    .withDescription("compress type of each column. 1: LZ4, 2: DELTAOFDELTA");

    public static final Option<DataSaveMode> DATA_SAVE_MODE =
            Options.key("data_save_mode")
                    .enumType(DataSaveMode.class)
                    .defaultValue(KEEP_SCHEMA_AND_DATA)
                    .withDescription("data_save_mode");

    public static final Option<SchemaSaveMode> SCHEMA_SAVE_MODE =
            Options.key("schema_save_mode")
                    .enumType(SchemaSaveMode.class)
                    .defaultValue(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST)
                    .withDescription("schema_save_mode");

    public static final Option<String> SAVE_MODE_CREATE_TEMPLATE =
            Options.key("save_mode_create_template")
                    .stringType()
                    .defaultValue(
                            "create table '${database}'.'${table_name}'(\n"
                                    + "     ${rowtype_fields}\n"
                                    + " )\n"
                                    + " partitioned by ${rowtype_primary_key};")
                    .withDescription(
                            "Create table statement template, used to create DolphinDB table");

    public static final Option<String> CUSTOM_SQL =
            Options.key("custom_sql").stringType().noDefaultValue().withDescription("custom_sql");
}
