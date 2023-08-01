package org.apache.seatunnel.connectors.seatunnel.common.multitablesink;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.sink.SinkWriter;

public class SinkContextProxy implements SinkWriter.Context {

    private final int index;

    private final SinkWriter.Context context;

    public SinkContextProxy(int index, SinkWriter.Context context) {
        this.index = index;
        this.context = context;
    }

    @Override
    public int getIndexOfSubtask() {
        return index;
    }

    @Override
    public MetricsContext getMetricsContext() {
        return context.getMetricsContext();
    }
}
