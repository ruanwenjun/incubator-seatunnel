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

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.connectors.seatunnel.redshift.RedshiftJdbcClient;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConf;

import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;

@Slf4j
public class S3RedshiftSaveModeHandler implements AutoCloseable {
    private final S3RedshiftSQLGenerator sqlGenerator;
    private final S3RedshiftConf conf;
    private final RedshiftJdbcClient redshiftJdbcClient;

    public S3RedshiftSaveModeHandler(S3RedshiftSQLGenerator sqlGenerator, S3RedshiftConf conf) {
        this.sqlGenerator = sqlGenerator;
        this.conf = conf;
        this.redshiftJdbcClient = RedshiftJdbcClient.newSingleConnection(conf);
    }

    public void handle(DataSaveMode saveMode) throws SQLException {
        if (conf.notAppendOnlyMode()) {
            if (conf.isCopyS3FileToTemporaryTableMode()) {
                redshiftJdbcClient.execute(sqlGenerator.getDropTemporaryTableSql());
                log.info("Drop temporary table: {}", sqlGenerator.getDropTemporaryTableSql());
                redshiftJdbcClient.execute(sqlGenerator.getCreateTemporaryTableSQL());
                log.info("Create temporary table: {}", sqlGenerator.getCreateTemporaryTableSQL());
            }
        }

        switch (saveMode) {
            case DROP_SCHEMA:
                dropSchema();
                break;
            case KEEP_SCHEMA_DROP_DATA:
                keepSchemaDropData();
                break;
            case KEEP_SCHEMA_AND_DATA:
                keepSchemaAndData();
                break;
            case CUSTOM_PROCESSING:
                customProcessing();
                break;
            case ERROR_WHEN_EXISTS:
                errorWhenExists();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported save mode: " + saveMode);
        }
    }

    private void dropSchema() throws SQLException {
        redshiftJdbcClient.execute(sqlGenerator.getDropTableSql());
        log.info("Drop table: {}", sqlGenerator.getDropTableSql());
        redshiftJdbcClient.execute(sqlGenerator.getCreateTableSQL());
        log.info("Create table: {}", sqlGenerator.getCreateTableSQL());
    }

    private void keepSchemaDropData() throws SQLException {
        if (redshiftJdbcClient.existDataForSql(sqlGenerator.generateIsExistTableSql())) {
            redshiftJdbcClient.execute(sqlGenerator.generateCleanTableSql());
            log.info("Clean table: {}", sqlGenerator.generateCleanTableSql());
        }
    }

    private void keepSchemaAndData() throws SQLException {
        redshiftJdbcClient.execute(sqlGenerator.getCreateTableSQL());
        log.info("Create table: {}", sqlGenerator.getCreateTableSQL());
    }

    private void customProcessing() throws SQLException {
        redshiftJdbcClient.execute(conf.getCustomSql());
        log.info("Execute custom sql: {}", conf.getCustomSql());
        redshiftJdbcClient.execute(sqlGenerator.getCreateTableSQL());
        log.info("Create table: {}", sqlGenerator.getCreateTableSQL());
    }

    private void errorWhenExists() throws SQLException {
        if (redshiftJdbcClient.executeQueryCount(sqlGenerator.getIsExistTableSql()) > 0) {
            if (redshiftJdbcClient.executeQueryCount(sqlGenerator.getIsExistDataSql()) > 0) {
                throw new IllegalStateException("The target data source already has data");
            }
        }
        redshiftJdbcClient.execute(sqlGenerator.getCreateTableSQL());
        log.info("Create table: {}", sqlGenerator.getCreateTableSQL());
    }

    @Override
    public void close() {
        redshiftJdbcClient.close();
    }
}
