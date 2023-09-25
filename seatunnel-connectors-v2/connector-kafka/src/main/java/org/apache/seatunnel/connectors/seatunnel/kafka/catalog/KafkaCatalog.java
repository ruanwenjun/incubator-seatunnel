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

package org.apache.seatunnel.connectors.seatunnel.kafka.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.Config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This is a KafkaCatalog implementation.
 *
 * <p>In kafka the database and table both are the topic name.
 */
@Slf4j
public class KafkaCatalog implements Catalog {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaCatalog.class);
    private final String catalogName;
    private final String bootstrapServers;
    private final ReadonlyConfig options;

    private AdminClient adminClient;

    public KafkaCatalog(String catalogName, ReadonlyConfig options, String bootstrapServers) {
        this.catalogName = catalogName;
        this.bootstrapServers = checkNotNull(bootstrapServers, "bootstrapServers cannot be null");
        this.options = checkNotNull(options, "options cannot be null");
    }

    @Override
    public void open() throws CatalogException {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        adminClient = AdminClient.create(properties);
    }

    @Override
    public void close() throws CatalogException {
        adminClient.close();
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @SneakyThrows
    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Map<String, TopicListing> topicListingMap = listTopicsResult.namesToListings().get();
        return new ArrayList<>(topicListingMap.keySet());
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        String topic = options.get(Config.TOPIC);
        final boolean aDefault = listTables("default").contains(topic);
        log.info("kafka tableExists topic {} : {}", topic, aDefault);
        return aDefault;
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        TableSchema tableSchema = CatalogTableUtil.buildWithConfig(options).getTableSchema();
        return CatalogTable.of(
                TableIdentifier.of(catalogName, "default", "default"),
                tableSchema,
                Collections.emptyMap(),
                Collections.emptyList(),
                "");
    }

    @SneakyThrows
    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        NewTopic newTopic =
                new NewTopic(
                        options.get(Config.TOPIC),
                        options.get(Config.TOPIC_PARTITIONS_NUM),
                        Short.parseShort(options.get(Config.TOPIC_REPLICATION_NUM).toString()));
        CreateTopicsResult topicsResult =
                adminClient.createTopics(Collections.singletonList(newTopic));
        topicsResult.all().get();
        log.info("kafka create topic {} success", options.get(Config.TOPIC));
    }

    @SneakyThrows
    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {}

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {}

    public boolean isExistsData(TablePath tablePath) {
        return false;
    }

    public void executeSql(String sql) {}
}
