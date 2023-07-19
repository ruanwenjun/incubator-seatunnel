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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.redshift.sink.S3RedshiftChangelogMode;
import org.apache.seatunnel.connectors.seatunnel.redshift.sink.S3RedshiftTemporaryTableMode;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

@Builder
@Getter
@ToString
public class S3RedshiftConf implements Serializable {
    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;
    private final String database;
    private final String executeSql;

    private final String s3Bucket;
    private final String accessKey;
    private final String secretKey;
    private final FileFormat fileFormat;

    private final S3RedshiftChangelogMode changelogMode;
    private final int changelogBufferFlushSize;
    private final int changelogBufferFlushInterval;
    @Setter private String redshiftTable;
    @Setter private List<String> redshiftTablePrimaryKeys;
    private final S3RedshiftTemporaryTableMode redshiftTemporaryTableMode;
    private final String redshiftTemporaryTableName;
    private final String redshiftExternalSchema;
    private final String redshiftS3IamRole;

    public boolean isAppendOnlyMode() {
        return S3RedshiftChangelogMode.APPEND_ONLY.equals(changelogMode);
    }

    public boolean isCopyS3FileToTemporaryTableMode() {
        return S3RedshiftTemporaryTableMode.S3_FILE_COPY_TEMPORARY_TABLE.equals(
                redshiftTemporaryTableMode);
    }

    public boolean isS3ExternalTableMode() {
        return S3RedshiftTemporaryTableMode.S3_EXTERNAL_TABLE.equals(redshiftTemporaryTableMode);
    }

    public static S3RedshiftConf valueOf(Config config) {
        return valueOf(ReadonlyConfig.fromConfig(config));
    }

    public static S3RedshiftConf valueOf(ReadonlyConfig readonlyConfig) {
        S3RedshiftConfBuilder builder = S3RedshiftConf.builder();

        checkPath(
                readonlyConfig.get(BaseSinkConfig.FILE_PATH),
                readonlyConfig.get(BaseSinkConfig.TMP_PATH));

        builder.jdbcUrl(readonlyConfig.get(S3RedshiftConfig.JDBC_URL));
        builder.jdbcUser(readonlyConfig.get(S3RedshiftConfig.JDBC_USER));
        builder.jdbcPassword(readonlyConfig.get(S3RedshiftConfig.JDBC_PASSWORD));
        builder.database(readonlyConfig.get(S3RedshiftConfig.DATABASE));
        builder.executeSql(readonlyConfig.get(S3RedshiftConfig.EXECUTE_SQL));

        builder.s3Bucket(readonlyConfig.get(S3RedshiftConfig.S3_BUCKET));
        builder.accessKey(readonlyConfig.get(S3RedshiftConfig.S3_ACCESS_KEY));
        builder.secretKey(readonlyConfig.get(S3RedshiftConfig.S3_SECRET_KEY));
        builder.fileFormat(readonlyConfig.get(S3RedshiftConfig.FILE_FORMAT_TYPE));

        builder.changelogMode(readonlyConfig.get(S3RedshiftConfig.CHANGELOG_MODE));
        builder.redshiftTemporaryTableMode(
                readonlyConfig.get(S3RedshiftConfig.REDSHIFT_TEMPORARY_TABLE_MODE));
        builder.redshiftTable(readonlyConfig.get(S3RedshiftConfig.REDSHIFT_TABLE));
        builder.redshiftTablePrimaryKeys(
                readonlyConfig.get(S3RedshiftConfig.REDSHIFT_TABLE_PRIMARY_KEYS));
        builder.changelogBufferFlushSize(
                readonlyConfig.get(S3RedshiftConfig.CHANGELOG_BUFFER_FLUSH_SIZE));
        builder.changelogBufferFlushInterval(
                readonlyConfig.get(S3RedshiftConfig.CHANGELOG_BUFFER_FLUSH_INTERVAL));
        builder.redshiftTemporaryTableMode(
                readonlyConfig.get(S3RedshiftConfig.REDSHIFT_TEMPORARY_TABLE_MODE));
        builder.redshiftTemporaryTableName(
                readonlyConfig.get(S3RedshiftConfig.REDSHIFT_TEMPORARY_TABLE_NAME));
        builder.redshiftExternalSchema(
                readonlyConfig.get(S3RedshiftConfig.REDSHIFT_EXTERNAL_SCHEMA));
        builder.redshiftS3IamRole(readonlyConfig.get(S3RedshiftConfig.REDSHIFT_S3_IAM_ROLE));


        if (!S3RedshiftChangelogMode.APPEND_ONLY.equals(builder.changelogMode)) {
            checkFormat(builder.fileFormat);
        }
        return builder.build();
    }

    public String getTemporaryTableName() {
        return getRedshiftTemporaryTableName()
                .replace("${redshift_table}", getRedshiftTable().replace(".", "_"));
    }

    private static void checkFormat(FileFormat fileFormat) throws IllegalArgumentException {
        if (!FileFormat.ORC.equals(fileFormat)) {
            throw new IllegalArgumentException(
                    "Only orc file format is supported for changelog mode");
        }
    }

    private static void checkPath(String... paths) throws IllegalArgumentException {
        for (String path : paths) {
            if (path == null || path.isEmpty()) {
                throw new IllegalArgumentException("Path cannot be empty");
            }
            if (!path.startsWith("/")) {
                throw new IllegalArgumentException("Path must start with /");
            }
        }
    }
}
