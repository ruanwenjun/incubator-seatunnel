package org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source.reader.fetch.snapshot;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.config.OracleAgentSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source.reader.fetch.OracleAgentSourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source.reader.fetch.incremental.OracleAgentIncrementalSplitFetchTask;

import org.whaleops.whaletunnel.oracleagent.sdk.OracleAgentClientFactory;

import io.debezium.connector.oracle.OracleAgentOffsetContext;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.spi.SnapshotResult;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

@Slf4j
public class OracleAgentSnapshotFetchTask implements FetchTask<SourceSplitBase> {

    private final OracleAgentSourceConfig oracleAgentSourceConfig;
    private final SnapshotSplit split;
    private volatile boolean taskRunning = false;

    public OracleAgentSnapshotFetchTask(
            OracleAgentSourceConfig oracleAgentSourceConfig, SnapshotSplit split) {
        this.oracleAgentSourceConfig = oracleAgentSourceConfig;
        this.split = split;
    }

    @Override
    public void execute(Context context) throws Exception {
        OracleAgentSourceFetchTaskContext sourceFetchTaskContext =
                (OracleAgentSourceFetchTaskContext) context;
        taskRunning = true;
        OracleAgentSnapshotSplitReadTask snapshotSplitReadTask =
                new OracleAgentSnapshotSplitReadTask(
                        sourceFetchTaskContext.getDbzConnectorConfig(),
                        sourceFetchTaskContext.getOffsetContext(),
                        sourceFetchTaskContext.getSnapshotChangeEventSourceMetrics(),
                        sourceFetchTaskContext.getDatabaseSchema(),
                        sourceFetchTaskContext.getConnection(),
                        sourceFetchTaskContext.getDispatcher(),
                        split,
                        OracleAgentClientFactory.getOrCreateStartedSocketClient(
                                oracleAgentSourceConfig.getOracleAgentHost(),
                                oracleAgentSourceConfig.getOracleAgentPort()));

        OracleAgentSnapshotSplitChangeEventSourceContext changeEventSourceContext =
                new OracleAgentSnapshotSplitChangeEventSourceContext();
        SnapshotResult snapshotResult =
                snapshotSplitReadTask.execute(
                        changeEventSourceContext, sourceFetchTaskContext.getOffsetContext());
        if (!snapshotResult.isCompletedOrSkipped()) {
            taskRunning = false;
            throw new IllegalStateException(
                    String.format("Read snapshot for split %s fail", split));
        }

        boolean changed =
                changeEventSourceContext
                        .getHighWatermark()
                        .isAfter(changeEventSourceContext.getLowWatermark());
        if (!context.isExactlyOnce()) {
            taskRunning = false;
            if (changed) {
                log.debug("Skip merge changelog(exactly-once) for snapshot split {}", split);
            }
            return;
        }

        final IncrementalSplit backfillSplit =
                createBackfillOracle9BridgeLogSplit(changeEventSourceContext);
        // optimization that skip the binlog read when the low watermark equals high
        // watermark
        if (!changed) {
            log.info(
                    "Skip merge changelog(exactly-once) for snapshot split {}, since the watermark doesn't changed",
                    split);
            dispatchBinlogEndEvent(
                    backfillSplit,
                    sourceFetchTaskContext.getOffsetContext().getPartition(),
                    sourceFetchTaskContext.getDispatcher());
            taskRunning = false;
            return;
        }
        // execute redoLog read task
        final OracleAgentIncrementalSplitFetchTask incrementalSplitFetchTask =
                createIncrementalOracle9BridgeLogReadTask(backfillSplit, sourceFetchTaskContext);

        long startTime = System.currentTimeMillis();
        log.info(
                "Start execute OracleAgentIncrementalSplitFetchTask, start offset : {}, stop offset : {}",
                backfillSplit.getStartupOffset(),
                backfillSplit.getStopOffset());

        OracleAgentOffsetContext.Loader loader =
                new OracleAgentOffsetContext.Loader(
                        sourceFetchTaskContext.getSourceConfig().getDbzConnectorConfig());
        OracleAgentOffsetContext incrementalSplitFetchTaskContext =
                loader.load(backfillSplit.getStartupOffset().getOffset());

        incrementalSplitFetchTask.execute(
                new SnapshotScnSplitChangeEventSourceContext(), incrementalSplitFetchTaskContext);
        log.info(
                "End execute OracleAgentIncrementalSplitFetchTask cost: {}/ms",
                System.currentTimeMillis() - startTime);

        taskRunning = false;
    }

    private IncrementalSplit createBackfillOracle9BridgeLogSplit(
            OracleAgentSnapshotSplitChangeEventSourceContext sourceContext) {
        return new IncrementalSplit(
                split.splitId(),
                Collections.singletonList(split.getTableId()),
                sourceContext.getLowWatermark(),
                sourceContext.getHighWatermark(),
                new ArrayList<>());
    }

    private OracleAgentIncrementalSplitFetchTask createIncrementalOracle9BridgeLogReadTask(
            IncrementalSplit backfillBinlogSplit, OracleAgentSourceFetchTaskContext context) {

        OracleAgentOffsetContext.Loader loader =
                new OracleAgentOffsetContext.Loader(
                        context.getSourceConfig().getDbzConnectorConfig());
        OracleAgentOffsetContext oracleAgentOffsetContext =
                loader.load(backfillBinlogSplit.getStartupOffset().getOffset());

        return new OracleAgentIncrementalSplitFetchTask(
                oracleAgentOffsetContext,
                context.getDbzConnectorConfig(),
                context.getConnection(),
                context.getSourceConfig(),
                context.getDispatcher(),
                context.getErrorHandler(),
                context.getDatabaseSchema(),
                backfillBinlogSplit);
    }

    private void dispatchBinlogEndEvent(
            IncrementalSplit backFillBinlogSplit,
            Map<String, ?> sourcePartition,
            JdbcSourceEventDispatcher eventDispatcher)
            throws InterruptedException {
        eventDispatcher.dispatchWatermarkEvent(
                sourcePartition,
                backFillBinlogSplit,
                backFillBinlogSplit.getStopOffset(),
                WatermarkKind.END);
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public void shutdown() {
        taskRunning = false;
    }

    @Override
    public SourceSplitBase getSplit() {
        return split;
    }

    /**
     * The {@link ChangeEventSource.ChangeEventSourceContext} implementation for bounded binlog task
     * of a snapshot split task.
     */
    public class SnapshotScnSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {

        public void finished() {
            taskRunning = false;
        }

        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
