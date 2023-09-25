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

package org.apache.seatunnel.connectors.seatunnel.redshift.sink;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3Config;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConf;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConfig;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.api.sink.SinkCommonOptions.MULTI_TABLE_SINK_REPLICA;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_DATABASE_NAME_KEY;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_SCHEMA_NAME_KEY;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_TABLE_NAME_KEY;

@AutoService(Factory.class)
public class S3RedshiftFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "S3Redshift";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        S3Config.S3_BUCKET,
                        S3RedshiftConfig.JDBC_URL,
                        S3RedshiftConfig.JDBC_USER,
                        S3RedshiftConfig.JDBC_PASSWORD,
                        S3RedshiftConfig.SCHEMA_NAME,
                        S3RedshiftConfig.SCHEMA_SAVE_MODE,
                        S3RedshiftConfig.DATA_SAVE_MODE,
                        BaseSinkConfig.FILE_PATH,
                        BaseSinkConfig.TMP_PATH,
                        S3Config.S3A_AWS_CREDENTIALS_PROVIDER,
                        MULTI_TABLE_SINK_REPLICA)
                .conditional(
                        S3Config.S3A_AWS_CREDENTIALS_PROVIDER,
                        S3Config.S3aAwsCredentialsProvider.SimpleAWSCredentialsProvider,
                        S3Config.S3_ACCESS_KEY,
                        S3Config.S3_SECRET_KEY)
                .conditional(
                        S3Config.S3A_AWS_CREDENTIALS_PROVIDER,
                        S3Config.S3aAwsCredentialsProvider.InstanceProfileCredentialsProvider,
                        S3RedshiftConfig.REDSHIFT_S3_IAM_ROLE)
                .conditional(
                        S3RedshiftConfig.DATA_SAVE_MODE,
                        DataSaveMode.CUSTOM_PROCESSING,
                        S3RedshiftConfig.CUSTOM_SQL)
                .optional(S3Config.S3_PROPERTIES)
                .optional(
                        S3RedshiftConfig.CHANGELOG_MODE,
                        S3RedshiftConfig.REDSHIFT_TABLE,
                        S3RedshiftConfig.REDSHIFT_TABLE_PRIMARY_KEYS,
                        S3RedshiftConfig.REDSHIFT_S3_FILE_COMMIT_WORKER_SIZE)
                .conditional(S3RedshiftConfig.CHANGELOG_MODE, S3RedshiftChangelogMode.APPEND_ONLY)
                .conditional(
                        S3RedshiftConfig.CHANGELOG_MODE,
                        S3RedshiftChangelogMode.APPEND_ON_DUPLICATE_UPDATE,
                        S3RedshiftConfig.CHANGELOG_BUFFER_FLUSH_SIZE,
                        S3RedshiftConfig.CHANGELOG_BUFFER_FLUSH_INTERVAL,
                        S3RedshiftConfig.REDSHIFT_TEMPORARY_TABLE_NAME)
                .conditional(
                        S3RedshiftConfig.CHANGELOG_MODE,
                        S3RedshiftChangelogMode.APPEND_ON_DUPLICATE_UPDATE_AUTOMATIC,
                        S3RedshiftConfig.CHANGELOG_BUFFER_FLUSH_SIZE,
                        S3RedshiftConfig.CHANGELOG_BUFFER_FLUSH_INTERVAL,
                        S3RedshiftConfig.REDSHIFT_TEMPORARY_TABLE_NAME)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        CatalogTable catalogTable = context.getCatalogTable();
        ReadonlyConfig config = context.getOptions();
        S3RedshiftConf s3RedshiftConf = S3RedshiftConf.valueOf(config);
        // get source table relevant information
        TableIdentifier tableId = catalogTable.getTableId();
        String sourceDatabaseName = tableId.getDatabaseName();
        String sourceSchemaName = tableId.getSchemaName();
        String sourceTableName = tableId.getTableName();
        // get sink table relevant information
        String sinkDatabaseName = s3RedshiftConf.getDatabase();
        String sinkTableNameBefore = s3RedshiftConf.getRedshiftTable();
        if (StringUtils.isEmpty(sinkTableNameBefore)) {
            sinkTableNameBefore = REPLACE_TABLE_NAME_KEY;
        }
        String[] sinkTableSplitArray = sinkTableNameBefore.split("\\.");
        String sinkTableName = sinkTableSplitArray[sinkTableSplitArray.length - 1];
        String sinkSchemaName;
        if (sinkTableSplitArray.length > 1) {
            sinkSchemaName = sinkTableSplitArray[sinkTableSplitArray.length - 2];
        } else {
            sinkSchemaName = null;
        }
        if (StringUtils.isNotEmpty(s3RedshiftConf.getSchema())) {
            sinkSchemaName = s3RedshiftConf.getSchema();
        }
        // to replace
        String finalDatabaseName =
                sinkDatabaseName.replace(REPLACE_DATABASE_NAME_KEY, sourceDatabaseName);
        String finalSchemaName;
        if (sinkSchemaName != null) {
            if (sourceSchemaName == null) {
                finalSchemaName = sinkSchemaName;
            } else {
                finalSchemaName = sinkSchemaName.replace(REPLACE_SCHEMA_NAME_KEY, sourceSchemaName);
            }
        } else {
            finalSchemaName = null;
        }
        String finalTableName = sinkTableName.replace(REPLACE_TABLE_NAME_KEY, sourceTableName);
        // rebuild TableIdentifier and catalogTable
        TableIdentifier newTableId =
                TableIdentifier.of(
                        tableId.getCatalogName(),
                        finalDatabaseName,
                        finalSchemaName,
                        finalTableName);
        catalogTable =
                CatalogTable.of(
                        newTableId,
                        catalogTable.getTableSchema(),
                        catalogTable.getOptions(),
                        catalogTable.getPartitionKeys(),
                        catalogTable.getCatalogName());

        CatalogTable finalCatalogTable = catalogTable;
        // reset
        s3RedshiftConf.setRedshiftTable(finalTableName);
        s3RedshiftConf.setDatabase(finalDatabaseName);
        s3RedshiftConf.setSchema(finalSchemaName);
        if (CollectionUtils.isEmpty(s3RedshiftConf.getRedshiftTablePrimaryKeys())) {
            TableSchema tableSchema = catalogTable.getTableSchema();
            if (tableSchema.getPrimaryKey() != null) {
                s3RedshiftConf.setRedshiftTablePrimaryKeys(
                        tableSchema.getPrimaryKey().getColumnNames());
            }
        }

        return () ->
                new S3RedshiftSink(
                        finalCatalogTable,
                        s3RedshiftConf,
                        ConfigFactory.parseMap(config.toMap()),
                        config);
    }
}
