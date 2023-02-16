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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.catalog;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.ElasticsearchClusterInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.IndexDocsCount;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Elasticsearch catalog implementation.
 * <p>In ElasticSearch, we use the index as the database and table.
 */
public class ElasticSearchCatalog implements Catalog {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchCatalog.class);

    private final String catalogName;
    private final String defaultDatabase;
    private final Config pluginConfig;

    private EsRestClient esRestClient;

    // todo: do we need default database?
    public ElasticSearchCatalog(String catalogName, String defaultDatabase, Config elasticSearchConfig) {
        this.catalogName = checkNotNull(catalogName, "catalogName cannot be null");
        this.defaultDatabase = defaultDatabase;
        this.pluginConfig = checkNotNull(elasticSearchConfig, "elasticSearchConfig cannot be null");
    }

    @Override
    public void open() throws CatalogException {
        try {
            esRestClient = EsRestClient.createInstance(pluginConfig);
            ElasticsearchClusterInfo elasticsearchClusterInfo = esRestClient.getClusterInfo();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Success open es catalog: {}, cluster info: {}", catalogName, elasticsearchClusterInfo);
            }
        } catch (Exception e) {
            throw new CatalogException(String.format("Failed to open catalog %s", catalogName), e);
        }
    }

    @Override
    public void close() throws CatalogException {
        esRestClient.close();
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return defaultDatabase;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        // check if the index exist
        try {
            List<IndexDocsCount> indexDocsCount = esRestClient.getIndexDocsCount(databaseName);
            return true;
        } catch (Exception e) {
            throw new CatalogException(
                String.format("Failed to check if catalog %s database %s exists", catalogName, databaseName), e);
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return esRestClient.listIndex();
    }

    @Override
    public List<String> listTables(String databaseName) throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        }
        return Lists.newArrayList(databaseName);
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        checkNotNull(tablePath);
        return databaseExists(tablePath.getTableName());
    }

    @Override
    public CatalogTable getTable(TablePath tablePath) throws CatalogException, TableNotExistException {
        checkNotNull(tablePath);
        if (!Objects.equals(tablePath.getTableName(), tablePath.getDatabaseName())) {
            throw new IllegalArgumentException(String.format("table name: %s and database name: %s must be the same", tablePath.getTableName(), tablePath.getDatabaseName()));
        }
        // todo: Get the index mapping and shard info?
        CatalogOptionBuilder catalogOptionBuilder = CatalogOptionBuilder.of(pluginConfig);
        TableIdentifier tableIdentifier = TableIdentifier.of(catalogName, tablePath.getDatabaseName(), tablePath.getTableName());
        TableSchema tableSchema = TableSchema.builder().build();
        List<String> partitionKeys = Collections.emptyList();
        String comment = "";
        return CatalogTable.of(
            tableIdentifier,
            tableSchema,
            catalogOptionBuilder.buildCatalogOption(),
            partitionKeys,
            comment
        );
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        // todo: create the index with the options in catalogTable
        if (databaseExists(tablePath.getTableName())) {
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(catalogName, tablePath.getTableName());
            }
        } else {
            try {
                esRestClient.createIndex(tablePath.getTableName());
            } catch (Exception ex) {
                throw new CatalogException(
                    String.format("Failed to create table %s in catalog %s", tablePath.getTableName(), catalogName), ex);
            }
        }

    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        checkNotNull(tablePath);
        if (!tableExists(tablePath) && !ignoreIfNotExists) {
            throw new TableNotExistException(catalogName, tablePath);
        }
        try {
            esRestClient.dropIndex(tablePath.getTableName());
        } catch (Exception ex) {
            throw new CatalogException(
                String.format("Failed to drop table %s in catalog %s", tablePath.getTableName(), catalogName), ex);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists) throws DatabaseAlreadyExistException, CatalogException {
        createTable(tablePath, null, ignoreIfExists);
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists) throws DatabaseNotExistException, CatalogException {
        dropTable(tablePath, ignoreIfNotExists);
    }
}
