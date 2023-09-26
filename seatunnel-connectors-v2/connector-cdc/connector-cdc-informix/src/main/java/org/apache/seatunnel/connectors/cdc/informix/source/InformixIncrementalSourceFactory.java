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

package org.apache.seatunnel.connectors.cdc.informix.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.informix.config.InformixSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AutoService(Factory.class)
public class InformixIncrementalSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return InformixIncrementalSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return JdbcSourceOptions.getBaseRule()
                .required(
                        JdbcSourceOptions.USERNAME,
                        JdbcSourceOptions.PASSWORD,
                        JdbcSourceOptions.DATABASE_NAMES,
                        CatalogOptions.TABLE_NAMES,
                        JdbcCatalogOptions.BASE_URL)
                .optional(
                        JdbcSourceOptions.PORT,
                        JdbcSourceOptions.SERVER_TIME_ZONE,
                        JdbcSourceOptions.CONNECT_TIMEOUT_MS,
                        JdbcSourceOptions.CONNECT_MAX_RETRIES,
                        JdbcSourceOptions.CONNECTION_POOL_SIZE,
                        InformixSourceOptions.STARTUP_MODE)
                .conditional(
                        InformixSourceOptions.STARTUP_MODE,
                        StartupMode.INITIAL,
                        SourceOptions.EXACTLY_ONCE)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> {
            SeaTunnelDataType<SeaTunnelRow> dataType;
            List<CatalogTable> catalogTables =
                    CatalogTableUtil.getCatalogTablesFromConfig(
                            context.getOptions(), context.getClassLoader());
            if (catalogTables.size() == 1) {
                dataType = catalogTables.get(0).getTableSchema().toPhysicalRowDataType();
            } else {
                Map<String, SeaTunnelRowType> rowTypeMap = new HashMap<>();
                for (CatalogTable catalogTable : catalogTables) {
                    rowTypeMap.put(
                            catalogTable.getTableId().toTablePath().toString(),
                            catalogTable.getTableSchema().toPhysicalRowDataType());
                }
                dataType = new MultipleRowType(rowTypeMap);
            }
            return (SeaTunnelSource<T, SplitT, StateT>)
                    new InformixIncrementalSource<>(context.getOptions(), dataType, catalogTables);
        };
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return InformixIncrementalSource.class;
    }
}
