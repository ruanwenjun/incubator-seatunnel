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

package io.debezium.connector.dameng;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DamengOffsetContext implements OffsetContext {
    public static final String SERVER_PARTITION_KEY = "server";
    public static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";
    private final Schema sourceInfoSchema;
    private final SourceInfo sourceInfo;
    private final Map<String, String> partition;
    private final TransactionContext transactionContext;
    private boolean snapshotCompleted;

    public DamengOffsetContext(
            DamengConnectorConfig connectorConfig,
            Scn scn,
            Scn commitScn,
            String lcrPosition,
            boolean snapshot,
            boolean snapshotCompleted,
            TransactionContext transactionContext) {
        this(connectorConfig, scn, lcrPosition, snapshot, snapshotCompleted, transactionContext);
        sourceInfo.setCommitScn(commitScn);
    }

    public DamengOffsetContext(
            DamengConnectorConfig connectorConfig,
            Scn scn,
            String lcrPosition,
            boolean snapshot,
            boolean snapshotCompleted,
            TransactionContext transactionContext) {
        partition =
                Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());

        sourceInfo = new SourceInfo(connectorConfig);
        sourceInfo.setScn(scn);
        sourceInfo.setLcrPosition(lcrPosition);
        sourceInfoSchema = sourceInfo.schema();

        this.transactionContext = transactionContext;

        this.snapshotCompleted = snapshotCompleted;
        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        } else {
            sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
    }

    public static class Builder {

        private DamengConnectorConfig connectorConfig;
        private Scn scn;
        private String lcrPosition;
        private boolean snapshot;
        private boolean snapshotCompleted;
        private TransactionContext transactionContext;

        public DamengOffsetContext.Builder logicalName(DamengConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
            return this;
        }

        public DamengOffsetContext.Builder scn(Scn scn) {
            this.scn = scn;
            return this;
        }

        public DamengOffsetContext.Builder lcrPosition(String lcrPosition) {
            this.lcrPosition = lcrPosition;
            return this;
        }

        public DamengOffsetContext.Builder snapshot(boolean snapshot) {
            this.snapshot = snapshot;
            return this;
        }

        public DamengOffsetContext.Builder snapshotCompleted(boolean snapshotCompleted) {
            this.snapshotCompleted = snapshotCompleted;
            return this;
        }

        public DamengOffsetContext.Builder transactionContext(
                TransactionContext transactionContext) {
            this.transactionContext = transactionContext;
            return this;
        }

        DamengOffsetContext build() {
            return new DamengOffsetContext(
                    connectorConfig,
                    scn,
                    lcrPosition,
                    snapshot,
                    snapshotCompleted,
                    transactionContext);
        }
    }

    public static DamengOffsetContext.Builder create() {
        return new DamengOffsetContext.Builder();
    }

    @Override
    public Map<String, ?> getPartition() {
        return partition;
    }

    @Override
    public Map<String, ?> getOffset() {
        if (sourceInfo.isSnapshot()) {
            Map<String, Object> offset = new HashMap<>();

            final Scn scn = sourceInfo.getScn();
            offset.put(SourceInfo.SCN_KEY, scn != null ? scn.toString() : scn);
            offset.put(SourceInfo.SNAPSHOT_KEY, true);
            offset.put(SNAPSHOT_COMPLETED_KEY, snapshotCompleted);

            return offset;
        } else {
            final Map<String, Object> offset = new HashMap<>();
            if (sourceInfo.getLcrPosition() != null) {
                offset.put(SourceInfo.LCR_POSITION_KEY, sourceInfo.getLcrPosition());
            } else {
                final Scn scn = sourceInfo.getScn();
                final Scn commitScn = sourceInfo.getCommitScn();
                offset.put(SourceInfo.SCN_KEY, scn != null ? scn.toString() : null);
                offset.put(
                        SourceInfo.COMMIT_SCN_KEY, commitScn != null ? commitScn.toString() : null);
            }
            return transactionContext.store(offset);
        }
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    public void setScn(Scn scn) {
        sourceInfo.setScn(scn);
    }

    public void setCommitScn(Scn commitScn) {
        sourceInfo.setCommitScn(commitScn);
    }

    public Scn getScn() {
        return sourceInfo.getScn();
    }

    public Scn getCommitScn() {
        return sourceInfo.getCommitScn();
    }

    public void setLcrPosition(String lcrPosition) {
        sourceInfo.setLcrPosition(lcrPosition);
    }

    public String getLcrPosition() {
        return sourceInfo.getLcrPosition();
    }

    public void setTransactionId(String transactionId) {
        sourceInfo.setTransactionId(transactionId);
    }

    public void setSourceTime(Instant instant) {
        sourceInfo.setSourceTime(instant);
    }

    public void setTableId(TableId tableId) {
        sourceInfo.tableEvent(tableId);
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot() && !snapshotCompleted;
    }

    @Override
    public void preSnapshotStart() {
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
        snapshotCompleted = false;
    }

    @Override
    public void preSnapshotCompletion() {
        snapshotCompleted = true;
    }

    @Override
    public void postSnapshotCompletion() {
        sourceInfo.setSnapshot(SnapshotRecord.FALSE);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("OracleOffsetContext [scn=").append(getScn());

        if (sourceInfo.isSnapshot()) {
            sb.append(", snapshot=").append(sourceInfo.isSnapshot());
            sb.append(", snapshot_completed=").append(snapshotCompleted);
        }

        sb.append("]");

        return sb.toString();
    }

    @Override
    public void markLastSnapshotRecord() {
        sourceInfo.setSnapshot(SnapshotRecord.LAST);
    }

    @Override
    public void event(DataCollectionId tableId, Instant timestamp) {
        sourceInfo.tableEvent((TableId) tableId);
        sourceInfo.setSourceTime(timestamp);
    }

    public void tableEvent(TableId tableId, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.tableEvent(tableId);
    }

    public void tableEvent(Set<TableId> tableIds, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.tableEvent(tableIds);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    /**
     * Helper method to resolve a {@link Scn} by key from the offset map.
     *
     * @param offset the offset map
     * @param key the entry key, either {@link SourceInfo#SCN_KEY} or {@link
     *     SourceInfo#COMMIT_SCN_KEY}.
     * @return the {@link Scn} or null if not found
     */
    public static Scn getScnFromOffsetMapByKey(Map<String, ?> offset, String key) {
        Object scn = offset.get(key);
        if (scn instanceof String) {
            return Scn.valueOf((String) scn);
        } else if (scn != null) {
            return Scn.valueOf((Long) scn);
        }
        return null;
    }
}
