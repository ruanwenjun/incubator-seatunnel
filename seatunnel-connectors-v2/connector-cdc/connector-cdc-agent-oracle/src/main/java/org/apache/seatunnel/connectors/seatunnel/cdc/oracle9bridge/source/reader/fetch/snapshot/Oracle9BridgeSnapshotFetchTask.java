package org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.source.reader.fetch.snapshot;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.config.Oracle9BridgeSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.source.reader.fetch.Oracle9BridgeSourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.source.reader.fetch.incremental.Oracle9BridgeIncrementalSplitFetchTask;

import org.whaleops.whaletunnel.oracleagent.sdk.OracleAgentClientFactory;

import io.debezium.connector.oracle.Oracle9BridgeOffsetContext;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.spi.SnapshotResult;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

@Slf4j
public class Oracle9BridgeSnapshotFetchTask implements FetchTask<SourceSplitBase> {

    private final Oracle9BridgeSourceConfig oracle9BridgeSourceConfig;
    private final SnapshotSplit split;
    private volatile boolean taskRunning = false;

    public Oracle9BridgeSnapshotFetchTask(
            Oracle9BridgeSourceConfig oracle9BridgeSourceConfig, SnapshotSplit split) {
        this.oracle9BridgeSourceConfig = oracle9BridgeSourceConfig;
        this.split = split;
    }

    @Override
    public void execute(Context context) throws Exception {
        Oracle9BridgeSourceFetchTaskContext sourceFetchTaskContext =
                (Oracle9BridgeSourceFetchTaskContext) context;
        taskRunning = true;
        Oracle9BridgeSnapshotSplitReadTask snapshotSplitReadTask =
                new Oracle9BridgeSnapshotSplitReadTask(
                        sourceFetchTaskContext.getDbzConnectorConfig(),
                        sourceFetchTaskContext.getOffsetContext(),
                        sourceFetchTaskContext.getSnapshotChangeEventSourceMetrics(),
                        sourceFetchTaskContext.getDatabaseSchema(),
                        sourceFetchTaskContext.getConnection(),
                        sourceFetchTaskContext.getDispatcher(),
                        split,
                        OracleAgentClientFactory.getOrCreateStartedSocketClient(
                                oracle9BridgeSourceConfig.getOracle9BridgeHost(),
                                oracle9BridgeSourceConfig.getOracle9BridgePort()));

        Oracle9BridgeSnapshotSplitChangeEventSourceContext changeEventSourceContext =
                new Oracle9BridgeSnapshotSplitChangeEventSourceContext();
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
        final Oracle9BridgeIncrementalSplitFetchTask incrementalSplitFetchTask =
                createIncrementalOracle9BridgeLogReadTask(backfillSplit, sourceFetchTaskContext);

        long startTime = System.currentTimeMillis();
        log.info(
                "Start execute Oracle9BridgeIncrementalSplitFetchTask, start offset : {}, stop offset : {}",
                backfillSplit.getStartupOffset(),
                backfillSplit.getStopOffset());

        Oracle9BridgeOffsetContext.Loader loader =
                new Oracle9BridgeOffsetContext.Loader(
                        sourceFetchTaskContext.getSourceConfig().getDbzConnectorConfig());
        Oracle9BridgeOffsetContext incrementalSplitFetchTaskContext =
                loader.load(backfillSplit.getStartupOffset().getOffset());

        incrementalSplitFetchTask.execute(
                new SnapshotScnSplitChangeEventSourceContext(), incrementalSplitFetchTaskContext);
        log.info(
                "End execute Oracle9BridgeIncrementalSplitFetchTask cost: {}/ms",
                System.currentTimeMillis() - startTime);

        taskRunning = false;
    }

    private IncrementalSplit createBackfillOracle9BridgeLogSplit(
            Oracle9BridgeSnapshotSplitChangeEventSourceContext sourceContext) {
        return new IncrementalSplit(
                split.splitId(),
                Collections.singletonList(split.getTableId()),
                sourceContext.getLowWatermark(),
                sourceContext.getHighWatermark(),
                new ArrayList<>());
    }

    private Oracle9BridgeIncrementalSplitFetchTask createIncrementalOracle9BridgeLogReadTask(
            IncrementalSplit backfillBinlogSplit, Oracle9BridgeSourceFetchTaskContext context) {

        Oracle9BridgeOffsetContext.Loader loader =
                new Oracle9BridgeOffsetContext.Loader(
                        context.getSourceConfig().getDbzConnectorConfig());
        Oracle9BridgeOffsetContext oracle9BridgeOffsetContext =
                loader.load(backfillBinlogSplit.getStartupOffset().getOffset());

        return new Oracle9BridgeIncrementalSplitFetchTask(
                oracle9BridgeOffsetContext,
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
