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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source.offset;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.config.OracleAgentSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source.OracleAgentDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.utils.OracleAgentClientUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.utils.OracleConnectionUtils;

import org.whaleops.whaletunnel.oracleagent.sdk.OracleAgentClient;
import org.whaleops.whaletunnel.oracleagent.sdk.OracleAgentClientFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
/** An offset factory class create {@link OracleAgentOffset} instance. */
public class OracleAgentOffsetFactory extends OffsetFactory {

    private static final long serialVersionUID = 1L;

    private final OracleAgentSourceConfig sourceConfig;

    private final OracleAgentDialect dialect;

    public OracleAgentOffsetFactory(
            OracleAgentSourceConfig sourceConfig, OracleAgentDialect dialect) {
        this.sourceConfig = sourceConfig;
        this.dialect = dialect;
    }

    @Override
    public Offset earliest() {
        log.info("Begin to get the earliest offset for OracleAgent");
        // todo: How to query the earliest fzs number from the oracle9bridge server?
        OracleAgentClient oracle9BridgeClient =
                OracleAgentClientFactory.getOrCreateStartedSocketClient(
                        sourceConfig.getOracleAgentHost(), sourceConfig.getOracleAgentPort());
        List<String> tables =
                dialect.discoverDataCollections(sourceConfig).stream()
                        .map(TableId::table)
                        .collect(Collectors.toList());

        try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
            List<String> tableOwners =
                    tables.stream()
                            .map(
                                    table ->
                                            OracleConnectionUtils.getTableOwner(
                                                    jdbcConnection, table))
                            .collect(Collectors.toList());
            Integer minFzsFileNumber =
                    OracleAgentClientUtils.currentMinFzsFileNumber(
                            oracle9BridgeClient, tableOwners, tables);
            Long minScn =
                    OracleAgentClientUtils.currentMinScn(
                            oracle9BridgeClient, tableOwners, tables, minFzsFileNumber);
            log.info(
                    "Get the min fzs file number: {}, min scn: {} for tables: {}",
                    minFzsFileNumber,
                    minScn,
                    tables);
            return new OracleAgentOffset(minFzsFileNumber, minScn);
        } catch (Exception e) {
            throw new RuntimeException("Read the binlog offset error", e);
        }
    }

    @Override
    public Offset neverStop() {
        return OracleAgentOffset.NO_STOPPING_OFFSET;
    }

    @Override
    public Offset latest() {
        log.info("Begin to get the latest offset for OracleAgent");
        OracleAgentClient oracle9BridgeClient =
                OracleAgentClientFactory.getOrCreateStartedSocketClient(
                        sourceConfig.getOracleAgentHost(), sourceConfig.getOracleAgentPort());
        List<String> tables =
                dialect.discoverDataCollections(sourceConfig).stream()
                        .map(TableId::table)
                        .collect(Collectors.toList());

        try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
            // todo: batch query the table owner
            List<String> tableOwners =
                    tables.stream()
                            .map(
                                    table ->
                                            OracleConnectionUtils.getTableOwner(
                                                    jdbcConnection, table))
                            .collect(Collectors.toList());
            Integer maxFzsFileNumber =
                    OracleAgentClientUtils.currentMaxFzsFileNumber(
                            oracle9BridgeClient, tableOwners, tables);
            Long maxScn =
                    OracleAgentClientUtils.currentMaxScn(
                            oracle9BridgeClient, tableOwners, tables, maxFzsFileNumber);
            log.info(
                    "Get the max fzs file number: {}, max scn: {} for tables: {}",
                    maxFzsFileNumber,
                    maxScn,
                    tables);
            return new OracleAgentOffset(maxFzsFileNumber, maxScn);
        } catch (Exception e) {
            throw new RuntimeException("Read the binlog offset error", e);
        }
    }

    @Override
    public Offset specific(Map<String, String> offset) {
        return new OracleAgentOffset(offset);
    }

    @Override
    public Offset specific(String filename, Long position) {
        throw new UnsupportedOperationException(
                "not supported create new Offset by filename and position.");
    }

    @Override
    public Offset timestamp(long timestamp) {
        throw new UnsupportedOperationException("not supported create new Offset by timestamp.");
    }
}
