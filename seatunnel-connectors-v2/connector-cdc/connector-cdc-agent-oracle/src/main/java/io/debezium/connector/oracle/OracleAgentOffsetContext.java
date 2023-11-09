package io.debezium.connector.oracle;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Collect;
import lombok.RequiredArgsConstructor;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

public class OracleAgentOffsetContext implements OffsetContext {
    public static final String SERVER_PARTITION_KEY = "server";
    private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";
    private final Schema sourceInfoSchema;
    private final SourceInfo sourceInfo;
    private final Map<String, String> partition;
    private final TransactionContext transactionContext;
    private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
    private boolean snapshotCompleted;

    public OracleAgentOffsetContext(
            OracleAgentConnectorConfig connectorConfig,
            Integer fzsFileNumber,
            Scn scn,
            boolean snapshot,
            boolean snapshotCompleted,
            TransactionContext transactionContext) {
        this(
                connectorConfig,
                fzsFileNumber,
                scn,
                snapshot,
                snapshotCompleted,
                transactionContext,
                new IncrementalSnapshotContext());
    }

    public OracleAgentOffsetContext(
            OracleAgentConnectorConfig connectorConfig,
            Integer fzsFileNumber,
            Scn scn,
            boolean snapshot,
            boolean snapshotCompleted,
            TransactionContext transactionContext,
            IncrementalSnapshotContext incrementalSnapshotContext) {
        partition =
                Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        sourceInfo = new SourceInfo(connectorConfig);
        sourceInfo.setScn(scn);
        sourceInfo.setFzsFileNumber(fzsFileNumber);
        sourceInfoSchema = sourceInfo.schema();
        this.snapshotCompleted = snapshotCompleted;

        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        } else {
            sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;
    }

    @Override
    public Map<String, ?> getPartition() {
        return partition;
    }

    @Override
    public Map<String, ?> getOffset() {
        if (sourceInfo.isSnapshot()) {
            return Collect.hashMapOf(
                    SNAPSHOT_COMPLETED_KEY,
                    snapshotCompleted,
                    SourceInfo.SNAPSHOT_KEY,
                    true,
                    SourceInfo.SCN_KEY,
                    sourceInfo.getScn().longValue());
        }
        return incrementalSnapshotContext.store(
                transactionContext.store(
                        Collect.hashMapOf(
                                SourceInfo.SCN_KEY,
                                sourceInfo.getScn() == null
                                        ? null
                                        : sourceInfo.getScn().longValue(),
                                SourceInfo.FZS_FILE_NUMBER_KEY,
                                sourceInfo.getFzsFileNumber() == null
                                        ? null
                                        : sourceInfo.getFzsFileNumber())));
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot() && !snapshotCompleted;
    }

    @Override
    public void markLastSnapshotRecord() {
        sourceInfo.setSnapshot(SnapshotRecord.LAST);
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
    public void event(DataCollectionId collectionId, Instant timestamp) {
        sourceInfo.tableEvent((TableId) collectionId);
        sourceInfo.setSourceTime(timestamp);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public void incrementalSnapshotEvents() {
        sourceInfo.setSnapshot(SnapshotRecord.INCREMENTAL);
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }

    public void setScn(Scn changeScn) {
        sourceInfo.setScn(changeScn);
    }

    public Scn getScn() {
        return sourceInfo.getScn();
    }

    public void setFzsFileNumber(Integer fzsFileNumber) {
        sourceInfo.setFzsFileNumber(fzsFileNumber);
    }

    public Integer getFzsFileNumber() {
        return sourceInfo.getFzsFileNumber();
    }

    @RequiredArgsConstructor
    public static class Loader implements OffsetContext.Loader<OracleAgentOffsetContext> {
        private final OracleAgentConnectorConfig connectorConfig;

        @Override
        public Map<String, ?> getPartition() {
            return Collections.singletonMap(
                    OracleAgentOffsetContext.SERVER_PARTITION_KEY,
                    connectorConfig.getLogicalName());
        }

        @Override
        public OracleAgentOffsetContext load(Map<String, ?> offset) {
            Scn scn = getLsn(offset, SourceInfo.SCN_KEY);
            boolean snapshot = Boolean.TRUE.equals(offset.get(SourceInfo.SNAPSHOT_KEY));
            boolean snapshotCompleted = Boolean.TRUE.equals(offset.get(SNAPSHOT_COMPLETED_KEY));

            String fzsFileNumberStr = (String) offset.get(SourceInfo.FZS_FILE_NUMBER_KEY);
            Integer fzsFileNumber = null;
            if (fzsFileNumberStr != null) {
                fzsFileNumber = Integer.parseInt(fzsFileNumberStr);
            }

            return new OracleAgentOffsetContext(
                    connectorConfig,
                    fzsFileNumber,
                    scn,
                    snapshot,
                    snapshotCompleted,
                    TransactionContext.load(offset));
        }

        private static Scn getLsn(Map<String, ?> offset, String key) {
            Object scn = offset.get(key);
            if (scn instanceof String) {
                return Scn.valueOf((String) scn);
            } else if (scn != null) {
                return Scn.valueOf((Long) scn);
            }
            return null;
        }
    }
}
