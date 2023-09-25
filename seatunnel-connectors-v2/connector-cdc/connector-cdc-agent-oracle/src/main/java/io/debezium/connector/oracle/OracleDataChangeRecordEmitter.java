package io.debezium.connector.oracle;

import io.debezium.connector.oracle.oracle9bridge.Oracle9BridgeDmlEntry;
import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.util.Clock;

import static com.google.common.base.Preconditions.checkNotNull;

public class OracleDataChangeRecordEmitter extends RelationalChangeRecordEmitter {

    private final Oracle9BridgeDmlEntry oracle9BridgeDmlEntry;

    public OracleDataChangeRecordEmitter(
            OffsetContext offsetContext, Clock clock, Oracle9BridgeDmlEntry oracle9BridgeDmlEntry) {
        super(offsetContext, clock);
        this.oracle9BridgeDmlEntry =
                checkNotNull(oracle9BridgeDmlEntry, "oracle9BridgeDmlEntry must not be null");
    }

    @Override
    protected Envelope.Operation getOperation() {
        switch (oracle9BridgeDmlEntry.getOperation()) {
            case INSERT:
                return Envelope.Operation.CREATE;
            case UPDATE:
                return Envelope.Operation.UPDATE;
            case DELETE:
                return Envelope.Operation.DELETE;
            default:
                throw new IllegalArgumentException(
                        "Received event of unexpected command type: "
                                + oracle9BridgeDmlEntry.getOperation());
        }
    }

    @Override
    protected Object[] getOldColumnValues() {
        return oracle9BridgeDmlEntry.getOldValues();
    }

    @Override
    protected Object[] getNewColumnValues() {
        return oracle9BridgeDmlEntry.getNewValues();
    }
}
