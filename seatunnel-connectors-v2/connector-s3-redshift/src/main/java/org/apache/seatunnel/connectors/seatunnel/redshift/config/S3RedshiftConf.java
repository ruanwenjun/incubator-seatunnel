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
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.redshift.sink.S3RedshiftChangelogMode;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig.FILE_FORMAT_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.redshift.sink.S3RedshiftChangelogMode.APPEND_ONLY;
import static org.apache.seatunnel.connectors.seatunnel.redshift.sink.S3RedshiftChangelogMode.APPEND_ON_DUPLICATE_UPDATE_AUTOMATIC;

@Builder
@Getter
@ToString
public class S3RedshiftConf implements Serializable {
    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;
    private final String executeSql;
    private final String schema;

    private final String s3Bucket;
    private final String accessKey;
    private final String secretKey;

    private final DataSaveMode saveMode;
    private final S3RedshiftChangelogMode changelogMode;
    private final int changelogBufferFlushSize;
    private final int changelogBufferFlushInterval;
    @Setter private String redshiftTable;
    @Setter private List<String> redshiftTablePrimaryKeys;
    private final String redshiftTemporaryTableName;
    private final String redshiftS3IamRole;
    private final int redshiftS3FileCommitWorkerSize;
    private final String customSql;

    public boolean isAppendOnlyMode() {
        return APPEND_ONLY.equals(changelogMode);
    }

    public boolean notAppendOnlyMode() {
        return !isAppendOnlyMode();
    }

    public boolean isAllowAppend() {
        return APPEND_ON_DUPLICATE_UPDATE_AUTOMATIC.equals(changelogMode);
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
        builder.schema(readonlyConfig.get(S3RedshiftConfig.SCHEMA_NAME));

        builder.s3Bucket(readonlyConfig.get(S3RedshiftConfig.S3_BUCKET));
        builder.accessKey(readonlyConfig.get(S3RedshiftConfig.S3_ACCESS_KEY));
        builder.secretKey(readonlyConfig.get(S3RedshiftConfig.S3_SECRET_KEY));

        builder.saveMode(readonlyConfig.get(S3RedshiftConfig.SAVE_MODE));
        builder.changelogMode(readonlyConfig.get(S3RedshiftConfig.CHANGELOG_MODE));
        builder.redshiftTable(readonlyConfig.get(S3RedshiftConfig.REDSHIFT_TABLE));
        builder.redshiftTablePrimaryKeys(
                readonlyConfig.get(S3RedshiftConfig.REDSHIFT_TABLE_PRIMARY_KEYS));
        builder.changelogBufferFlushSize(
                readonlyConfig.get(S3RedshiftConfig.CHANGELOG_BUFFER_FLUSH_SIZE));
        builder.changelogBufferFlushInterval(
                readonlyConfig.get(S3RedshiftConfig.CHANGELOG_BUFFER_FLUSH_INTERVAL));
        builder.redshiftTemporaryTableName(
                readonlyConfig.get(S3RedshiftConfig.REDSHIFT_TEMPORARY_TABLE_NAME));
        builder.redshiftS3IamRole(readonlyConfig.get(S3RedshiftConfig.REDSHIFT_S3_IAM_ROLE));
        builder.redshiftS3FileCommitWorkerSize(
                readonlyConfig.get(S3RedshiftConfig.REDSHIFT_S3_FILE_COMMIT_WORKER_SIZE));
        builder.customSql(readonlyConfig.get(S3RedshiftConfig.CUSTOM_SQL));
        return builder.build();
    }

    public String getTemporaryTableName() {
        return getRedshiftTemporaryTableName()
                .replace("${redshift_table}", getRedshiftTable().replace(".", "_"));
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

    public static Config enhanceS3RedshiftConfig(Config config) {
        return config.withValue(
                FILE_FORMAT_TYPE.key(), ConfigValueFactory.fromAnyRef(FileFormat.ORC.name()));
    }
}
