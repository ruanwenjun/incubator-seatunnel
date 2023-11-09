package io.debezium.connector.oracle;

import io.debezium.connector.oracle.oracleAgent.OracleAgentDmlEntry;
import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.util.Clock;

import static com.google.common.base.Preconditions.checkNotNull;

public class OracleDataChangeRecordEmitter extends RelationalChangeRecordEmitter {

    private final OracleAgentDmlEntry oracleAgentDmlEntry;

    public OracleDataChangeRecordEmitter(
            OffsetContext offsetContext, Clock clock, OracleAgentDmlEntry oracleAgentDmlEntry) {
        super(offsetContext, clock);
        this.oracleAgentDmlEntry =
                checkNotNull(oracleAgentDmlEntry, "oracleAgentDmlEntry must not be null");
    }

    @Override
    protected Envelope.Operation getOperation() {
        switch (oracleAgentDmlEntry.getOperation()) {
            case INSERT:
                return Envelope.Operation.CREATE;
            case UPDATE:
                return Envelope.Operation.UPDATE;
            case DELETE:
                return Envelope.Operation.DELETE;
            default:
                throw new IllegalArgumentException(
                        "Received event of unexpected command type: "
                                + oracleAgentDmlEntry.getOperation());
        }
    }

    @Override
    protected Object[] getOldColumnValues() {
        return oracleAgentDmlEntry.getOldValues();
    }

    @Override
    protected Object[] getNewColumnValues() {
        return oracleAgentDmlEntry.getNewValues();
    }
}
