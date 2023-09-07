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

package io.debezium.connector.dameng.logminer;

import io.debezium.connector.dameng.DamengConnectorConfig;
import io.debezium.connector.dameng.DamengOffsetContext;
import io.debezium.connector.dameng.Scn;
import io.debezium.connector.dameng.SourceInfo;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;

import java.util.Collections;
import java.util.Map;

/** @author Chris Cranford */
public class LogMinerOracleOffsetContextLoader
        implements OffsetContext.Loader<DamengOffsetContext> {

    private final DamengConnectorConfig connectorConfig;

    public LogMinerOracleOffsetContextLoader(DamengConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    @Override
    public Map<String, ?> getPartition() {
        return Collections.singletonMap(
                DamengOffsetContext.SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
    }

    @Override
    public DamengOffsetContext load(Map<String, ?> offset) {
        boolean snapshot = Boolean.TRUE.equals(offset.get(SourceInfo.SNAPSHOT_KEY));
        boolean snapshotCompleted =
                Boolean.TRUE.equals(offset.get(DamengOffsetContext.SNAPSHOT_COMPLETED_KEY));

        Scn scn = DamengOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
        Scn commitScn =
                DamengOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.COMMIT_SCN_KEY);
        return new DamengOffsetContext(
                connectorConfig,
                scn,
                commitScn,
                null,
                snapshot,
                snapshotCompleted,
                TransactionContext.load(offset));
    }
}
