package org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.source.reader.fetch.snapshot;

import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.source.offset.Oracle9BridgeOffset;

import io.debezium.pipeline.source.spi.ChangeEventSource;
import lombok.Getter;
import lombok.Setter;

/**
 * {@link ChangeEventSource.ChangeEventSourceContext} implementation that keeps low/high watermark
 * for each {@link SnapshotSplit}.
 */
public class Oracle9BridgeSnapshotSplitChangeEventSourceContext
        implements ChangeEventSource.ChangeEventSourceContext {

    @Getter @Setter private Oracle9BridgeOffset lowWatermark;

    @Getter @Setter private Oracle9BridgeOffset highWatermark;

    @Override
    public boolean isRunning() {
        return lowWatermark != null && highWatermark != null;
    }
}
