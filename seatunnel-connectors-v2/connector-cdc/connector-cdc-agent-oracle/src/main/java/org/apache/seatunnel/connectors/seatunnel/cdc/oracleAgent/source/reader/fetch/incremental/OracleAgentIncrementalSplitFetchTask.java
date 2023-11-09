package org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source.reader.fetch.incremental;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.config.OracleAgentSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source.offset.OracleAgentOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source.reader.fetch.snapshot.OracleAgentSnapshotFetchTask;

import org.whaleops.whaletunnel.oracleagent.sdk.model.OracleOperation;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleAgentConnectorConfig;
import io.debezium.connector.oracle.OracleAgentOffsetContext;
import io.debezium.connector.oracle.OracleAgentStreamingChangeEventSource;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source.offset.OracleAgentOffset.NO_STOPPING_OFFSET;

@Slf4j
public class OracleAgentIncrementalSplitFetchTask extends OracleAgentStreamingChangeEventSource {

    private final IncrementalSplit split;
    private final JdbcSourceEventDispatcher eventDispatcher;
    private final ErrorHandler errorHandler;
    private final List<String> tables;
    private ChangeEventSourceContext context;

    public OracleAgentIncrementalSplitFetchTask(
            OracleAgentOffsetContext offsetContext,
            OracleAgentConnectorConfig connectorConfig,
            OracleConnection oracleConnection,
            OracleAgentSourceConfig sourceConfig,
            JdbcSourceEventDispatcher eventDispatcher,
            ErrorHandler errorHandler,
            OracleDatabaseSchema oracleDatabaseSchema,
            IncrementalSplit incrementalSplit) {
        super(
                offsetContext,
                connectorConfig,
                oracleConnection,
                incrementalSplit.getTableIds(),
                sourceConfig,
                eventDispatcher,
                errorHandler,
                oracleDatabaseSchema);
        this.split = incrementalSplit;
        this.eventDispatcher = eventDispatcher;
        this.errorHandler = errorHandler;
        this.tables = split.getTableIds().stream().map(TableId::table).collect(Collectors.toList());
    }

    @Override
    public void execute(ChangeEventSourceContext context, OracleAgentOffsetContext offsetContext) {
        this.context = context;
        super.execute(context, offsetContext);
    }

    // todo: deal with recovery with entry index.
    @Override
    protected void handleEvent(
            OracleAgentOffsetContext offsetContext,
            Integer fzsFileNumber,
            List<OracleOperation> oracleOperations) {

        super.handleEvent(offsetContext, fzsFileNumber, oracleOperations);

        // check do we need to stop for fetch incremental log for snapshot split.
        if (isBoundedRead()) {
            OracleAgentOffset currentOffset = getCurrentOffset(offsetContext.getOffset());
            // reach the high watermark, the incremental fetcher should be finished
            if (currentOffset.isAtOrAfter(split.getStopOffset())) {
                // send incremental end event
                try {
                    log.info("Current offset is after split stopOffset: {}", split.getStopOffset());
                    eventDispatcher.dispatchWatermarkEvent(
                            offsetContext.getPartition(), split, currentOffset, WatermarkKind.END);
                } catch (InterruptedException e) {
                    log.error("Send signal event error.", e);
                    errorHandler.setProducerThrowable(
                            new DebeziumException("Error processing logminer signal event", e));
                }
                // tell fetcher the fzs task finished
                ((OracleAgentSnapshotFetchTask.SnapshotScnSplitChangeEventSourceContext) context)
                        .finished();
            } else {
                log.debug(
                        "Current offset: {} is before than split stopOffset: {}",
                        currentOffset,
                        split.getStopOffset());
            }
        }
    }

    private boolean isBoundedRead() {
        return !NO_STOPPING_OFFSET.equals(split.getStopOffset());
    }

    private static OracleAgentOffset getCurrentOffset(Map<String, ?> offset) {
        Map<String, String> offsetStrMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offset.entrySet()) {
            offsetStrMap.put(
                    entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
        }
        return new OracleAgentOffset(offsetStrMap);
    }
}
