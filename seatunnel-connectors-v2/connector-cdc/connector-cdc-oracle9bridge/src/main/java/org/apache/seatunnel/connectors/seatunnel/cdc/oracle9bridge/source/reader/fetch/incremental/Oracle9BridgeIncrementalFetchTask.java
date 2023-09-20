package org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.source.reader.fetch.incremental;

import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.config.Oracle9BridgeSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.source.reader.fetch.Oracle9BridgeSourceFetchTaskContext;

import io.debezium.connector.oracle.Oracle9BridgeStreamingChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Oracle9BridgeIncrementalFetchTask implements FetchTask<SourceSplitBase> {

    private Oracle9BridgeSourceConfig oracle9BridgeSourceConfig;
    private final IncrementalSplit incrementalSplit;
    private volatile boolean taskRunning = false;

    public Oracle9BridgeIncrementalFetchTask(
            Oracle9BridgeSourceConfig oracle9BridgeSourceConfig, IncrementalSplit split) {
        this.oracle9BridgeSourceConfig = oracle9BridgeSourceConfig;
        this.incrementalSplit = split;
    }

    @Override
    public void execute(Context context) {
        taskRunning = true;
        Oracle9BridgeSourceFetchTaskContext sourceFetchContext =
                (Oracle9BridgeSourceFetchTaskContext) context;

        Oracle9BridgeStreamingChangeEventSource oracle9BridgeStreamingChangeEventSource =
                new Oracle9BridgeStreamingChangeEventSource(
                        sourceFetchContext.getOffsetContext(),
                        sourceFetchContext.getConnection(),
                        incrementalSplit.getTableIds(),
                        sourceFetchContext.getSourceConfig(),
                        sourceFetchContext.getDispatcher(),
                        sourceFetchContext.getErrorHandler(),
                        sourceFetchContext.getDatabaseSchema());

        Oracle9BridgeIncrementalChangeEventSourceContext changeEventSourceContext =
                new Oracle9BridgeIncrementalChangeEventSourceContext();
        oracle9BridgeStreamingChangeEventSource.execute(
                changeEventSourceContext, sourceFetchContext.getOffsetContext());
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public void shutdown() {
        log.info("Shutdown the Oracle9BridgeIncrementalFetchTask");
        taskRunning = false;
    }

    @Override
    public SourceSplitBase getSplit() {
        return incrementalSplit;
    }

    /**
     * The {@link ChangeEventSource.ChangeEventSourceContext} implementation for binlog split task.
     */
    private class Oracle9BridgeIncrementalChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {
        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
