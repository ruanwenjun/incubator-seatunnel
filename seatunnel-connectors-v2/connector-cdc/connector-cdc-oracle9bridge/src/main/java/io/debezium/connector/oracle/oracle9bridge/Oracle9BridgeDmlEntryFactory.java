package io.debezium.connector.oracle.oracle9bridge;

import org.whaleops.whaletunnel.oracle9bridge.sdk.model.OracleDeleteOperation;
import org.whaleops.whaletunnel.oracle9bridge.sdk.model.OracleInsertOperation;
import org.whaleops.whaletunnel.oracle9bridge.sdk.model.OracleOperation;
import org.whaleops.whaletunnel.oracle9bridge.sdk.model.OracleQmiOperation;
import org.whaleops.whaletunnel.oracle9bridge.sdk.model.OracleUpdateOperation;

import io.debezium.relational.Column;
import io.debezium.relational.Table;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

public class Oracle9BridgeDmlEntryFactory {

    public static List<Oracle9BridgeDmlEntry> transformOperation(
            OracleOperation operation, Table table) {
        switch (operation.getType()) {
            case OracleInsertOperation.TYPE:
                return Collections.singletonList(
                        transformInsert((OracleInsertOperation) operation, table));
            case OracleUpdateOperation.TYPE:
                return Collections.singletonList(
                        transformUpdate((OracleUpdateOperation) operation, table));
            case OracleDeleteOperation.TYPE:
                return Collections.singletonList(
                        transformDelete((OracleDeleteOperation) operation, table));
            case OracleQmiOperation.TYPE:
                return transformBatchInsert((OracleQmiOperation) operation, table);
            default:
                throw new IllegalArgumentException(
                        "Unknown supported operation type: " + operation.getType());
        }
    }

    public static Oracle9BridgeDmlEntry transformInsert(
            OracleInsertOperation insertOperation, Table table) {
        Object[] newValues = getWholeColumnValues(insertOperation.getInsertRow(), table);
        return Oracle9BridgeDmlEntryImpl.forInsert(newValues);
    }

    public static Oracle9BridgeDmlEntry transformUpdate(
            OracleUpdateOperation updateOperation, Table table) {
        List<Column> columns = table.columns();
        Map<String, String> newRow = updateOperation.getUpdatedRow();
        Map<String, String> oldRow = updateOperation.getUpdateCondition();

        Object[] oldValues = getWholeColumnValues(oldRow, table);
        Object[] newValues = getWholeColumnValues(newRow, table);
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            if (newRow.containsKey(column.name())) {
                newValues[i] = transformToOracleType(newRow.get(column.name()), column);
            } else {
                newValues[i] = oldValues[i];
            }
        }
        return Oracle9BridgeDmlEntryImpl.forUpdate(newValues, oldValues);
    }

    public static Oracle9BridgeDmlEntry transformDelete(
            OracleDeleteOperation deleteOperation, Table table) {
        Object[] oldValues = getWholeColumnValues(deleteOperation.getDeletedRow(), table);
        return Oracle9BridgeDmlEntryImpl.forDelete(oldValues);
    }

    public static List<Oracle9BridgeDmlEntry> transformBatchInsert(
            OracleQmiOperation oracleQmiOperation, Table table) {
        return oracleQmiOperation.getInsertRows().stream()
                .map(
                        insertRow ->
                                Oracle9BridgeDmlEntryImpl.forInsert(
                                        getWholeColumnValues(insertRow, table)))
                .collect(Collectors.toList());
    }

    private static Object[] getWholeColumnValues(Map<String, String> columnValues, Table table) {
        List<Column> columns = table.columns();
        checkArgument(
                columnValues.size() == columns.size(), "Column values and columns size mismatch");
        Object[] objects = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            objects[i] = transformToOracleType(columnValues.get(column.name()), column);
        }
        return objects;
    }

    private static Object transformToOracleType(String value, Column column) {
        // todo: Do we need to translate to oracle type or use string is ok?
        return value;
    }
}
