package io.debezium.connector.oracle.oracle9bridge;

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
public class Oracle9BridgeDmlEntryImpl implements Oracle9BridgeDmlEntry {
    @NonNull private final Operation operation;
    private final Object[] newValues;
    private final Object[] oldValues;

    public static Oracle9BridgeDmlEntryImpl forInsert(Object[] newColumnValues) {
        return new Oracle9BridgeDmlEntryImpl(Operation.INSERT, newColumnValues, null);
    }

    public static Oracle9BridgeDmlEntryImpl forUpdate(
            Object[] newColumnValues, Object[] oldColumnValues) {
        return new Oracle9BridgeDmlEntryImpl(Operation.UPDATE, newColumnValues, oldColumnValues);
    }

    public static Oracle9BridgeDmlEntryImpl forDelete(Object[] oldColumnValues) {
        return new Oracle9BridgeDmlEntryImpl(Operation.DELETE, null, oldColumnValues);
    }
}
