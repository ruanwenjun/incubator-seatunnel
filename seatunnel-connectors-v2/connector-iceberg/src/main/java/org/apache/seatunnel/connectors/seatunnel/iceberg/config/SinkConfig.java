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

package org.apache.seatunnel.connectors.seatunnel.iceberg.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

import org.apache.iceberg.FileFormat;

import lombok.Getter;

import java.util.Arrays;
import java.util.List;

import static org.apache.seatunnel.api.sink.DataSaveMode.ERROR_WHEN_DATA_EXISTS;
import static org.apache.seatunnel.api.sink.DataSaveMode.KEEP_SCHEMA_AND_DATA;
import static org.apache.seatunnel.api.sink.DataSaveMode.KEEP_SCHEMA_DROP_DATA;

public class SinkConfig extends CommonConfig {
    private static final long serialVersionUID = 1L;

    public static final Option<Boolean> ENABLE_UPSERT =
            Options.key("enable_upsert")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("enable upsert");

    public static final Option<List<String>> PRIMARY_KEYS =
            Options.key("primary_keys").listType().noDefaultValue().withDescription("primary keys");

    public static final Option<FileFormat> FILE_FORMAT =
            Options.key("file_format")
                    .singleChoice(
                            FileFormat.class, Arrays.asList(FileFormat.PARQUET, FileFormat.ORC))
                    .defaultValue(FileFormat.PARQUET)
                    .withDescription("file format");

    public static final Option<Long> TARGET_FILE_SIZE_BYTES =
            Options.key("target_file_size_bytes")
                    .longType()
                    .defaultValue(128 * 1024 * 1024L)
                    .withDescription("target file size bytes");

    public static final Option<SchemaSaveMode> SCHEMA_SAVE_MODE =
            Options.key("schema_save_mode")
                    .enumType(SchemaSaveMode.class)
                    .defaultValue(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST)
                    .withDescription("schema_save_mode");

    public static final Option<DataSaveMode> DATA_SAVE_MODE =
            Options.key("data_save_mode")
                    .singleChoice(
                            DataSaveMode.class,
                            Arrays.asList(
                                    KEEP_SCHEMA_DROP_DATA,
                                    KEEP_SCHEMA_AND_DATA,
                                    ERROR_WHEN_DATA_EXISTS))
                    .defaultValue(KEEP_SCHEMA_AND_DATA)
                    .withDescription("data_save_mode");

    @Getter private boolean enableUpsert;
    @Getter private List<String> primaryKeys;
    @Getter private FileFormat fileFormat;
    @Getter private long targetFileSizeBytes;
    @Getter private SchemaSaveMode schemaSaveMode;
    @Getter private DataSaveMode dataSaveMode;

    public SinkConfig(ReadonlyConfig pluginConfig) {
        super(pluginConfig);
        this.enableUpsert = pluginConfig.get(ENABLE_UPSERT);
        this.primaryKeys = pluginConfig.get(PRIMARY_KEYS);
        this.fileFormat = pluginConfig.get(FILE_FORMAT);
        this.targetFileSizeBytes = pluginConfig.get(TARGET_FILE_SIZE_BYTES);
        this.schemaSaveMode = pluginConfig.get(SCHEMA_SAVE_MODE);
        this.dataSaveMode = pluginConfig.get(DATA_SAVE_MODE);
    }
}
