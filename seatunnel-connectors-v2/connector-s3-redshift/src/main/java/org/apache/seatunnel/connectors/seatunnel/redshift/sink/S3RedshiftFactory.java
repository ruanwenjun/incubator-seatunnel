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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3Config;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConf;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConfig;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

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
                        BaseSinkConfig.FILE_PATH,
                        BaseSinkConfig.TMP_PATH,
                        S3Config.S3A_AWS_CREDENTIALS_PROVIDER)
                .conditional(
                        S3Config.S3A_AWS_CREDENTIALS_PROVIDER,
                        S3Config.S3aAwsCredentialsProvider.SimpleAWSCredentialsProvider,
                        S3Config.S3_ACCESS_KEY,
                        S3Config.S3_SECRET_KEY)
                .optional(S3Config.S3_PROPERTIES)
                .optional(
                        S3RedshiftConfig.CHANGELOG_MODE,
                        S3RedshiftConfig.REDSHIFT_TABLE,
                        S3RedshiftConfig.REDSHIFT_TABLE_PRIMARY_KEYS,
                        S3RedshiftConfig.REDSHIFT_S3_IAM_ROLE,
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
                .conditional(
                        S3RedshiftConfig.CHANGELOG_MODE,
                        S3RedshiftChangelogMode.APPEND_ON_DUPLICATE_DELETE,
                        S3RedshiftConfig.CHANGELOG_BUFFER_FLUSH_SIZE,
                        S3RedshiftConfig.CHANGELOG_BUFFER_FLUSH_INTERVAL,
                        S3RedshiftConfig.REDSHIFT_TEMPORARY_TABLE_NAME)
                .conditional(
                        S3RedshiftConfig.CHANGELOG_MODE,
                        S3RedshiftChangelogMode.APPEND_ON_DUPLICATE_DELETE_AUTOMATIC,
                        S3RedshiftConfig.CHANGELOG_BUFFER_FLUSH_SIZE,
                        S3RedshiftConfig.CHANGELOG_BUFFER_FLUSH_INTERVAL,
                        S3RedshiftConfig.REDSHIFT_TEMPORARY_TABLE_NAME)
                .build();
    }

    @Override
    public TableSink createSink(TableFactoryContext context) {
        CatalogTable catalogTable = context.getCatalogTable();
        ReadonlyConfig config = context.getOptions();

        S3RedshiftConf s3RedshiftConf = S3RedshiftConf.valueOf(config);
        if (StringUtils.isBlank(s3RedshiftConf.getRedshiftTable())) {
            s3RedshiftConf.setRedshiftTable(catalogTable.getTableId().getTableName());

            TableSchema tableSchema = catalogTable.getTableSchema();
            s3RedshiftConf.setRedshiftTablePrimaryKeys(
                    tableSchema.getPrimaryKey().getColumnNames());
        }
        return () ->
                new S3RedshiftSink(
                        catalogTable,
                        s3RedshiftConf,
                        ConfigFactory.parseMap(config.toMap()),
                        config);
    }
}
