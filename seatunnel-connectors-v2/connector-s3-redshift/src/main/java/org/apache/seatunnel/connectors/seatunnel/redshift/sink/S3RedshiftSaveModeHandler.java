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
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.redshift.RedshiftJdbcClient;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConf;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class S3RedshiftSaveModeHandler extends DefaultSaveModeHandler {
    private final S3RedshiftSQLGenerator sqlGenerator;
    private final S3RedshiftConf conf;
    private final RedshiftJdbcClient redshiftJdbcClient;

    public S3RedshiftSaveModeHandler(
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            Catalog catalog,
            CatalogTable catalogTable,
            String customSql,
            S3RedshiftSQLGenerator sqlGenerator,
            S3RedshiftConf conf) {
        super(schemaSaveMode, dataSaveMode, catalog, catalogTable, customSql);
        this.sqlGenerator = sqlGenerator;
        this.conf = conf;
        this.redshiftJdbcClient = RedshiftJdbcClient.newSingleConnection(conf);
    }

    @SneakyThrows
    @Override
    public boolean tableExists() {
        return redshiftJdbcClient.existDataForSql(sqlGenerator.getIsExistTableSql());
    }

    @SneakyThrows
    @Override
    public void dropTable() {
        redshiftJdbcClient.execute(sqlGenerator.getDropTableSql());
        redshiftJdbcClient.execute(sqlGenerator.getDropTemporaryTableSql());
    }

    @SneakyThrows
    @Override
    public void createTable() {
        if (conf.notAppendOnlyMode()) {
            redshiftJdbcClient.execute(sqlGenerator.getDropTemporaryTableSql());
            redshiftJdbcClient.execute(sqlGenerator.getCreateTemporaryTableSQL());
        }
        redshiftJdbcClient.execute(sqlGenerator.getCreateTableSQL());
    }

    @SneakyThrows
    @Override
    public void truncateTable() {
        redshiftJdbcClient.execute(sqlGenerator.getCleanTableSql());
    }

    @SneakyThrows
    @Override
    public boolean dataExists() {
        return redshiftJdbcClient.existDataForSql(sqlGenerator.getIsExistDataSql());
    }

    @SneakyThrows
    @Override
    public void executeCustomSql() {
        redshiftJdbcClient.execute(conf.getCustomSql());
    }

    @Override
    public void close() {
        redshiftJdbcClient.close();
    }
}
