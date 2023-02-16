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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.EsClusterConnectionConfig;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import java.util.HashMap;
import java.util.Map;

public class CatalogOptionBuilder {

    private final Config config;

    private CatalogOptionBuilder(Config config) {
        this.config = config;
    }

    public static CatalogOptionBuilder of(Config config) {
        return new CatalogOptionBuilder(config);
    }

    public Map<String, Object> buildCatalogOption() {
        Map<String, Object> catalogOption = new HashMap<>();
        catalogOption.put("connector", "Elasticsearch");
        addOptionIfPresent(catalogOption, config, EsClusterConnectionConfig.HOSTS);
        addOptionIfPresent(catalogOption, config, EsClusterConnectionConfig.USERNAME);
        addOptionIfPresent(catalogOption, config, EsClusterConnectionConfig.PASSWORD);
        addOptionIfPresent(catalogOption, config, EsClusterConnectionConfig.TLS_VERIFY_CERTIFICATE);
        addOptionIfPresent(catalogOption, config, EsClusterConnectionConfig.TLS_KEY_STORE_PATH);
        addOptionIfPresent(catalogOption, config, EsClusterConnectionConfig.TLS_KEY_STORE_PASSWORD);
        addOptionIfPresent(catalogOption, config, EsClusterConnectionConfig.TLS_TRUST_STORE_PATH);
        addOptionIfPresent(catalogOption, config, EsClusterConnectionConfig.TLS_TRUST_STORE_PASSWORD);
        addOptionIfPresent(catalogOption, config, EsClusterConnectionConfig.TLS_VERIFY_HOSTNAME);
        return catalogOption;
    }

    public Config buildCatalogConfig(Map<String, Object> catalogOption) {
        return config.withFallback(ConfigFactory.parseMap(catalogOption));
    }

    private <T> void addOptionIfPresent(Map<String, Object> catalogOption, Config pluginConfig, Option<T> option) {
        if (pluginConfig.hasPath(option.key())) {
            catalogOption.put(option.key(), pluginConfig.getAnyRef(option.key()));
        } else {
            catalogOption.put(option.key(), option.defaultValue());
        }
    }

}
