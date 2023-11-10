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

package org.apache.seatunnel.connectors.seatunnel.file.local.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.SinkReplaceNameConstant;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;

import com.google.auto.service.AutoService;

import java.util.Map;

@AutoService(Factory.class)
public class LocalFileSinkFactory
        implements TableSinkFactory<
                        SeaTunnelRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo>,
                SupportMultiTableSink {

    @Override
    public String factoryIdentifier() {
        return FileSystemType.LOCAL.getFileSystemPluginName();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(BaseSinkConfig.FILE_PATH)
                .optional(BaseSinkConfig.FILE_FORMAT_TYPE)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.TEXT,
                        BaseSinkConfig.ROW_DELIMITER,
                        BaseSinkConfig.FIELD_DELIMITER,
                        BaseSinkConfig.TXT_COMPRESS,
                        BaseSinkConfig.ENABLE_HEADER_WRITE)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.CSV,
                        BaseSinkConfig.TXT_COMPRESS,
                        BaseSinkConfig.ENABLE_HEADER_WRITE)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.JSON,
                        BaseSinkConfig.TXT_COMPRESS)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.ORC,
                        BaseSinkConfig.ORC_COMPRESS)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.PARQUET,
                        BaseSinkConfig.PARQUET_COMPRESS)
                .optional(BaseSinkConfig.CUSTOM_FILENAME)
                .conditional(
                        BaseSinkConfig.CUSTOM_FILENAME,
                        true,
                        BaseSinkConfig.FILE_NAME_EXPRESSION,
                        BaseSinkConfig.FILENAME_TIME_FORMAT)
                .optional(BaseSinkConfig.HAVE_PARTITION)
                .conditional(
                        BaseSinkConfig.HAVE_PARTITION,
                        true,
                        BaseSinkConfig.PARTITION_BY,
                        BaseSinkConfig.PARTITION_DIR_EXPRESSION,
                        BaseSinkConfig.IS_PARTITION_FIELD_WRITE_IN_FILE)
                .optional(BaseSinkConfig.SINK_COLUMNS)
                .optional(BaseSinkConfig.IS_ENABLE_TRANSACTION)
                .optional(BaseSinkConfig.DATE_FORMAT)
                .optional(BaseSinkConfig.DATETIME_FORMAT)
                .optional(BaseSinkConfig.TIME_FORMAT)
                .build();
    }

    @Override
    public TableSink<SeaTunnelRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo>
            createSink(TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();

        ReadonlyConfig finalReadonlyConfig =
                generateCurrentReadonlyConfig(readonlyConfig, catalogTable);
        return () -> new LocalFileSink(finalReadonlyConfig, catalogTable);
    }

    // replace the table name in sink config's path
    private ReadonlyConfig generateCurrentReadonlyConfig(
            ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        // Copy the config to avoid modifying the original config
        readonlyConfig = readonlyConfig.clone();
        Map<String, Object> configMap = readonlyConfig.getConfData();

        readonlyConfig
                .getOptional(BaseSinkConfig.FILE_PATH)
                .ifPresent(
                        path -> {
                            String replacedPath = replaceCatalotTableInPath(path, catalogTable);
                            configMap.put(BaseSinkConfig.FILE_PATH.key(), replacedPath);
                        });

        readonlyConfig
                .getOptional(BaseSinkConfig.TMP_PATH)
                .ifPresent(
                        path -> {
                            String replacedPath = replaceCatalotTableInPath(path, catalogTable);
                            configMap.put(BaseSinkConfig.TMP_PATH.key(), replacedPath);
                        });

        return ReadonlyConfig.fromMap(configMap);
    }

    private String replaceCatalotTableInPath(String originString, CatalogTable catalogTable) {
        String path = originString;
        TableIdentifier tableIdentifier = catalogTable.getTableId();
        if (tableIdentifier != null) {
            if (tableIdentifier.getDatabaseName() != null) {
                path =
                        path.replace(
                                SinkReplaceNameConstant.REPLACE_DATABASE_NAME_KEY,
                                tableIdentifier.getDatabaseName());
            }
            if (tableIdentifier.getSchemaName() != null) {
                path =
                        path.replace(
                                SinkReplaceNameConstant.REPLACE_SCHEMA_NAME_KEY,
                                tableIdentifier.getSchemaName());
            }
            if (tableIdentifier.getTableName() != null) {
                path =
                        path.replace(
                                SinkReplaceNameConstant.REPLACE_TABLE_NAME_KEY,
                                tableIdentifier.getTableName());
            }
        }
        return path;
    }
}
