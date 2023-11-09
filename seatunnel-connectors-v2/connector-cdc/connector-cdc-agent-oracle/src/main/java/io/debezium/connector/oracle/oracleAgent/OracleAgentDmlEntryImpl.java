package io.debezium.connector.oracle.oracleAgent;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;

@Getter
@ToString
@Accessors(chain = true)
@EqualsAndHashCode(of = {"operation", "newValues", "oldValues"})
@RequiredArgsConstructor
public class OracleAgentDmlEntryImpl implements OracleAgentDmlEntry {
    @NonNull private final Operation operation;
    private final Object[] newValues;
    private final Object[] oldValues;

    public static OracleAgentDmlEntryImpl forInsert(Object[] newColumnValues) {
        return new OracleAgentDmlEntryImpl(Operation.INSERT, newColumnValues, null);
    }

    public static OracleAgentDmlEntryImpl forUpdate(
            Object[] newColumnValues, Object[] oldColumnValues) {
        return new OracleAgentDmlEntryImpl(Operation.UPDATE, newColumnValues, oldColumnValues);
    }

    public static OracleAgentDmlEntryImpl forDelete(Object[] oldColumnValues) {
        return new OracleAgentDmlEntryImpl(Operation.DELETE, null, oldColumnValues);
    }
}
