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

package org.apache.seatunnel.connectors.seatunnel.file.s3.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3Config;

import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_DATABASE_NAME_KEY;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_SCHEMA_NAME_KEY;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_TABLE_NAME_KEY;

@AutoService(Factory.class)
public class S3FileSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return FileSystemType.S3.getFileSystemPluginName();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(S3Config.FILE_PATH)
                .required(S3Config.S3_BUCKET)
                .required(S3Config.FS_S3A_ENDPOINT)
                .required(S3Config.S3A_AWS_CREDENTIALS_PROVIDER)
                .required(S3Config.SCHEMA_SAVE_MODE)
                .required(S3Config.DATA_SAVE_MODE)
                .conditional(
                        S3Config.S3A_AWS_CREDENTIALS_PROVIDER,
                        S3Config.S3aAwsCredentialsProvider.SimpleAWSCredentialsProvider,
                        S3Config.S3_ACCESS_KEY,
                        S3Config.S3_SECRET_KEY)
                .optional(S3Config.S3_PROPERTIES)
                .optional(BaseSinkConfig.FILE_FORMAT_TYPE)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.TEXT,
                        BaseSinkConfig.ROW_DELIMITER,
                        BaseSinkConfig.FIELD_DELIMITER,
                        BaseSinkConfig.TXT_COMPRESS)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.CSV,
                        BaseSinkConfig.TXT_COMPRESS)
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
    public TableSink createSink(TableSinkFactoryContext context) {
        final CatalogTable catalogTable = context.getCatalogTable();
        final ReadonlyConfig options = context.getOptions();
        // get source table relevant information
        TableIdentifier tableId = catalogTable.getTableId();
        String sourceDatabaseName =
                tableId.getDatabaseName() == null ? "" : tableId.getDatabaseName();
        String sourceSchemaName = tableId.getSchemaName() == null ? "" : tableId.getSchemaName();
        String sourceTableName = tableId.getTableName() == null ? "" : tableId.getTableName();
        // get sink path
        String path = options.get(S3Config.FILE_PATH);
        // to replace
        path = path.replace(REPLACE_DATABASE_NAME_KEY, sourceDatabaseName);
        path = path.replace(REPLACE_SCHEMA_NAME_KEY, sourceSchemaName);
        path = path.replace(REPLACE_TABLE_NAME_KEY, sourceTableName);
        // rebuild
        Map<String, Object> confData = options.getConfData();
        final HashMap<String, Object> stringObjectHashMap = new HashMap<>(confData);
        stringObjectHashMap.put(S3Config.FILE_PATH.key(), path);
        ReadonlyConfig finalConfig = ReadonlyConfig.fromMap(stringObjectHashMap);
        return () -> new S3FileSink(catalogTable, finalConfig);
    }
}
