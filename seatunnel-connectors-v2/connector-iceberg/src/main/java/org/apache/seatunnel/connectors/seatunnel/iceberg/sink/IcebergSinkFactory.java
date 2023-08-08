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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.iceberg.catalog.IcebergCatalog;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.HDFS_SITE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.HIVE_SITE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KERBEROS_KEYTAB_PATH;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KERBEROS_KRB5_CONF_PATH;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KERBEROS_PRINCIPAL;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_CATALOG_NAME;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_CATALOG_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_NAMESPACE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_TABLE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_URI;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_WAREHOUSE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.HIVE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig.ENABLE_UPSERT;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig.PRIMARY_KEYS;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig.SAVE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig.TABLE_PREFIX;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig.TABLE_SUFFIX;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig.TARGET_FILE_SIZE_BYTES;

@AutoService(Factory.class)
@Slf4j
public class IcebergSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Iceberg";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(KEY_CATALOG_NAME, KEY_CATALOG_TYPE, KEY_WAREHOUSE, KEY_NAMESPACE)
                .conditional(KEY_CATALOG_TYPE, HIVE, KEY_URI)
                .optional(
                        KERBEROS_PRINCIPAL,
                        KERBEROS_KEYTAB_PATH,
                        KERBEROS_KRB5_CONF_PATH,
                        HDFS_SITE_PATH,
                        HIVE_SITE_PATH)
                .optional(KEY_TABLE)
                .optional(ENABLE_UPSERT)
                .optional(SAVE_MODE)
                .optional(PRIMARY_KEYS)
                .optional(TARGET_FILE_SIZE_BYTES)
                .optional(TABLE_PREFIX, TABLE_SUFFIX)
                .build();
    }

    @Override
    public TableSink createSink(TableFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        SinkConfig sinkConfig = new SinkConfig(config);

        CatalogTable catalogTable = renameCatalogTable(sinkConfig, context.getCatalogTable());

        IcebergCatalogFactory catalogFactory =
                new IcebergCatalogFactory(
                        sinkConfig.getCatalogName(),
                        sinkConfig.getCatalogType(),
                        sinkConfig.getWarehouse(),
                        sinkConfig.getUri(),
                        sinkConfig.getKerberosPrincipal(),
                        sinkConfig.getKerberosKrb5ConfPath(),
                        sinkConfig.getKerberosKeytabPath(),
                        sinkConfig.getHdfsSitePath(),
                        sinkConfig.getHiveSitePath());

        try (IcebergCatalog icebergCatalog = new IcebergCatalog(catalogFactory, "iceberg")) {
            icebergCatalog.open();
            if (!icebergCatalog.tableExists(catalogTable.getTableId().toTablePath())) {
                log.info(
                        "table not exists, create table: {}",
                        catalogTable.getTableId().toTablePath());
                icebergCatalog.createTable(
                        catalogTable.getTableId().toTablePath(), catalogTable, false);
            }
        }

        return () -> new IcebergSink(catalogTable, config);
    }

    private CatalogTable renameCatalogTable(SinkConfig sinkConfig, CatalogTable catalogTable) {

        TableIdentifier tableId = catalogTable.getTableId();
        String tableName;
        if (StringUtils.isNotEmpty(sinkConfig.getTable())) {
            tableName = sinkConfig.getTable();
        } else {
            tableName = tableId.getTableName();
            if (StringUtils.isNotBlank(sinkConfig.getTablePrefix())) {
                tableName = sinkConfig.getTablePrefix() + tableName;
            }

            if (StringUtils.isNotBlank(sinkConfig.getTableSuffix())) {
                tableName = tableName + sinkConfig.getTableSuffix();
            }
        }

        TableIdentifier newTableId =
                TableIdentifier.of(
                        tableId.getCatalogName(),
                        sinkConfig.getNamespace(),
                        tableId.getSchemaName(),
                        tableName);

        return CatalogTable.of(newTableId, catalogTable);
    }
}
