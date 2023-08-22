/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.selectdb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

@Setter
@Getter
@ToString
public class SelectDBConfig implements Serializable {
    private static final int DEFAULT_SINK_MAX_RETRIES = 3;
    private static final int DEFAULT_SINK_BUFFER_SIZE = 10 * 1024 * 1024;
    private static final int DEFAULT_SINK_BUFFER_COUNT = 10000;
    // common option
    public static final Option<String> LOAD_URL =
            Options.key("load-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SelectDB load http address.");
    public static final Option<String> BASE_URL =
            Options.key("base-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SelectDB jdbc query address.");
    public static final Option<String> CLUSTER_NAME =
            Options.key("cluster-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SelectDB cluster name.");

    public static final Option<String> TABLE_IDENTIFIER =
            Options.key("table.identifier")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the jdbc table name.");
    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the jdbc user name.");
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the jdbc password.");

    // sink config options
    public static final Option<Integer> SINK_MAX_RETRIES =
            Options.key("sink.max-retries")
                    .intType()
                    .defaultValue(DEFAULT_SINK_MAX_RETRIES)
                    .withDescription("the max retry times if writing records to database failed.");
    public static final Option<Integer> SINK_BUFFER_SIZE =
            Options.key("sink.buffer-size")
                    .intType()
                    .defaultValue(DEFAULT_SINK_BUFFER_SIZE)
                    .withDescription("the buffer size to cache data for stream load.");
    public static final Option<Integer> SINK_BUFFER_COUNT =
            Options.key("sink.buffer-count")
                    .intType()
                    .defaultValue(DEFAULT_SINK_BUFFER_COUNT)
                    .withDescription("the buffer count to cache data for stream load.");
    public static final Option<String> SINK_LABEL_PREFIX =
            Options.key("sink.label-prefix")
                    .stringType()
                    .defaultValue("seatunnel")
                    .withDescription("the unique label prefix.");
    public static final Option<Boolean> SINK_ENABLE_DELETE =
            Options.key("sink.enable-delete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("whether to enable the delete function");
    public static final Option<DataSaveMode> SAVE_MODE =
            Options.key("save_mode")
                    .enumType(DataSaveMode.class)
                    .defaultValue(DataSaveMode.KEEP_SCHEMA_AND_DATA)
                    .withDescription("save_mode");

    public static final Option<String> CUSTOM_SQL =
            Options.key("custom_sql").stringType().noDefaultValue().withDescription("custom_sql");
    public static final Option<String> SAVE_MODE_CREATE_TEMPLATE =
            Options.key("save_mode_create_template")
                    .stringType()
                    .defaultValue(
                            "CREATE TABLE IF NOT EXISTS `${database}`.`${table_name}` (\n"
                                    + "${rowtype_fields}\n"
                                    + ") ENGINE=OLAP\n"
                                    + " UNIQUE KEY (${rowtype_primary_key})\n"
                                    + "DISTRIBUTED BY HASH (${rowtype_primary_key})")
                    .withDescription(
                            "Create table statement template, used to create StarRocks table");

    public static final Option<Integer> SINK_FLUSH_QUEUE_SIZE =
            Options.key("sink.flush.queue-size")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Queue length for async upload to object storage");

    public static final Option<Map<String, String>> SELECTDB_SINK_CONFIG_PREFIX =
            Options.key("selectdb.config")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "The parameter of the Copy Into data_desc. "
                                    + "The way to specify the parameter is to add the prefix `selectdb.config` to the original load parameter name ");

    private String loadUrl;
    private String jdbcUrl;
    private String clusterName;
    private String username;
    private String password;
    private String tableIdentifier;
    private Boolean enableDelete;
    private String labelPrefix;
    private Integer maxRetries;
    private Integer bufferSize;
    private Integer bufferCount;
    private Integer flushQueueSize;
    private Properties StageLoadProps;
    private DataSaveMode saveMode;
    private String customSql;
    private String saveModeCreateTemplate;

    public static SelectDBConfig loadConfig(ReadonlyConfig pluginConfig) {
        SelectDBConfig selectdbConfig = new SelectDBConfig();
        selectdbConfig.setLoadUrl(pluginConfig.get(LOAD_URL));
        selectdbConfig.setJdbcUrl(pluginConfig.get(BASE_URL));
        selectdbConfig.setClusterName(pluginConfig.get(CLUSTER_NAME));
        selectdbConfig.setUsername(pluginConfig.get(USERNAME));
        selectdbConfig.setCustomSql(pluginConfig.get(CUSTOM_SQL));
        selectdbConfig.setPassword(pluginConfig.get(PASSWORD));
        selectdbConfig.setTableIdentifier(pluginConfig.get(TABLE_IDENTIFIER));
        selectdbConfig.setStageLoadProps(parseCopyIntoProperties(pluginConfig));
        selectdbConfig.setLabelPrefix(pluginConfig.get(SINK_LABEL_PREFIX));
        selectdbConfig.setMaxRetries(pluginConfig.get(SINK_MAX_RETRIES));
        selectdbConfig.setBufferSize(pluginConfig.get(SINK_BUFFER_SIZE));
        selectdbConfig.setBufferCount(pluginConfig.get(SINK_BUFFER_COUNT));
        selectdbConfig.setEnableDelete(pluginConfig.get(SINK_ENABLE_DELETE));
        selectdbConfig.setFlushQueueSize(pluginConfig.get(SINK_FLUSH_QUEUE_SIZE));
        selectdbConfig.setSaveMode(pluginConfig.get(SAVE_MODE));
        selectdbConfig.setSaveModeCreateTemplate(pluginConfig.get(SAVE_MODE_CREATE_TEMPLATE));
        return selectdbConfig;
    }

    private static Properties parseCopyIntoProperties(ReadonlyConfig pluginConfig) {
        Properties stageLoadProps = new Properties();
        pluginConfig
                .getOptional(SELECTDB_SINK_CONFIG_PREFIX)
                .ifPresent(
                        stringStringMap ->
                                stringStringMap.forEach(
                                        (key, value) -> {
                                            final String configKey = key.toLowerCase();
                                            stageLoadProps.put(configKey, value);
                                        }));
        return stageLoadProps;
    }
}
