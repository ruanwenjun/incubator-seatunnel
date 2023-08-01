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

package org.apache.seatunnel.connectors.seatunnel.redshift.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3Config;
import org.apache.seatunnel.connectors.seatunnel.redshift.sink.S3RedshiftChangelogMode;
import org.apache.seatunnel.connectors.seatunnel.redshift.sink.S3RedshiftTemporaryTableMode;

import lombok.Builder;

import java.util.List;

@Builder
public class S3RedshiftConfig extends S3Config {

    public static final Option<String> JDBC_URL =
            Options.key("jdbc_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Redshift JDBC URL");

    public static final Option<String> JDBC_USER =
            Options.key("jdbc_user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Redshift JDBC user");

    public static final Option<String> JDBC_PASSWORD =
            Options.key("jdbc_password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Redshift JDBC password");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Redshift JDBC database");

    public static final Option<String> SCHEMA_NAME =
            Options.key("schema_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Redshift JDBC schema");

    public static final Option<String> EXECUTE_SQL =
            Options.key("execute_sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Redshift execute sql");

    public static final Option<S3RedshiftChangelogMode> CHANGELOG_MODE =
            Options.key("changelog_mode")
                    .enumType(S3RedshiftChangelogMode.class)
                    .defaultValue(S3RedshiftChangelogMode.APPEND_ONLY)
                    .withDescription("Redshift write data changelog mode");

    public static final Option<List<String>> REDSHIFT_TABLE_PRIMARY_KEYS =
            Options.key("redshift_table_primary_keys")
                    .listType(String.class)
                    .noDefaultValue()
                    .withDescription("Redshift table primary key fields");

    public static final Option<String> REDSHIFT_TABLE =
            Options.key("redshift_table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Redshift sink table");

    public static final Option<Integer> CHANGELOG_BUFFER_FLUSH_SIZE =
            Options.key("changelog_buffer_flush_size")
                    .intType()
                    .defaultValue(20000)
                    .withDescription("Flush connector memory buffer to s3 size");

    public static final Option<Integer> CHANGELOG_BUFFER_FLUSH_INTERVAL =
            Options.key("changelog_buffer_flush_interval_ms")
                    .intType()
                    .defaultValue(20000)
                    .withDescription("Flush connector memory buffer to s3 interval");

    public static final Option<S3RedshiftTemporaryTableMode> REDSHIFT_TEMPORARY_TABLE_MODE =
            Options.key("redshift_temporary_table_mode")
                    .enumType(S3RedshiftTemporaryTableMode.class)
                    .defaultValue(S3RedshiftTemporaryTableMode.S3_FILE_COPY_TEMPORARY_TABLE)
                    .withDescription("Redshift temporary table mode");

    public static final Option<String> REDSHIFT_TEMPORARY_TABLE_NAME =
            Options.key("redshift_temporary_table_name")
                    .stringType()
                    .defaultValue("st_temporary_${redshift_table}")
                    .withDescription("Redshift temporary table name");

    public static final Option<String> REDSHIFT_EXTERNAL_SCHEMA =
            Options.key("redshift_external_schema")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Redshift external schema");

    public static final Option<String> REDSHIFT_S3_IAM_ROLE =
            Options.key("redshift_s3_iam_role")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Redshift connect S3 iam role");
}
