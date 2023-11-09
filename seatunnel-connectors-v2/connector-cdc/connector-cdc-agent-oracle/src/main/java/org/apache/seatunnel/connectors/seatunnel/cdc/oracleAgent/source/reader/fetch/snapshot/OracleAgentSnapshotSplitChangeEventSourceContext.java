package org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source.reader.fetch.snapshot;

import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source.offset.OracleAgentOffset;

import io.debezium.pipeline.source.spi.ChangeEventSource;
import lombok.Getter;
import lombok.Setter;

/**
 * {@link ChangeEventSource.ChangeEventSourceContext} implementation that keeps low/high watermark
 * for each {@link SnapshotSplit}.
 */
public class OracleAgentSnapshotSplitChangeEventSourceContext
        implements ChangeEventSource.ChangeEventSourceContext {

    @Getter @Setter private OracleAgentOffset lowWatermark;

    @Getter @Setter private OracleAgentOffset highWatermark;

    @Override
    public boolean isRunning() {
        return lowWatermark != null && highWatermark != null;
    }
}
