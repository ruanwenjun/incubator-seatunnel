package org.apache.seatunnel.connectors.dws.guassdb.sink.writer;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.commons.collections4.CollectionUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class DwsGaussDBMemoryTable {

    private final Map<String, SeaTunnelRow> upsertRows = new ConcurrentHashMap<>();

    private final Map<String, SeaTunnelRow> deleteRows = new ConcurrentHashMap<>();

    private final SeaTunnelRowType seaTunnelRowType;

    private final List<String> primaryKeys;

    private final Function<SeaTunnelRow, String> primaryKeyExtractor;

    public DwsGaussDBMemoryTable(SeaTunnelRowType seaTunnelRowType, List<String> primaryKeys) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.primaryKeys = primaryKeys;
        this.primaryKeyExtractor = getPrimaryKeyExtractor(seaTunnelRowType, primaryKeys);
    }

    public void write(SeaTunnelRow element) {
        String primaryKey = primaryKeyExtractor.apply(element);
        switch (element.getRowKind()) {
            case UPDATE_BEFORE:
                // todo: how to deal with update before
                break;
            case INSERT:
            case UPDATE_AFTER:
                deleteRows.remove(primaryKey);
                upsertRows.put(primaryKey, element);
                break;
            case DELETE:
                upsertRows.remove(primaryKey);
                deleteRows.put(primaryKey, element);
                break;
        }
    }

    public Collection<SeaTunnelRow> getDeleteRows() {
        return deleteRows.values();
    }

    public Collection<SeaTunnelRow> getUpsertRows() {
        return upsertRows.values();
    }

    public void truncate() {
        upsertRows.clear();
        deleteRows.clear();
    }

    private Function<SeaTunnelRow, String> getPrimaryKeyExtractor(
            SeaTunnelRowType seaTunnelRowType, List<String> primaryKeys) {
        if (CollectionUtils.isEmpty(primaryKeys)) {
            throw new IllegalArgumentException("The primary key is empty");
        }
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        int[] primaryKeyIndexes = new int[primaryKeys.size()];
        for (int i = 0; i < primaryKeys.size(); i++) {
            primaryKeyIndexes[i] = -1;
            String primaryKey = primaryKeys.get(i);
            for (int j = 0; j < fieldNames.length; j++) {
                if (primaryKey.equals(fieldNames[j])) {
                    primaryKeyIndexes[i] = j;
                    break;
                }
            }
            if (primaryKeyIndexes[i] == -1) {
                throw new IllegalArgumentException(
                        "The primary key: " + primaryKey + " in not exist in the table");
            }
        }
        return (row) -> {
            StringBuilder primaryKeyBuilder = new StringBuilder();
            for (int primaryKeyIndex : primaryKeyIndexes) {
                primaryKeyBuilder.append(row.getField(primaryKeyIndex));
            }
            return primaryKeyBuilder.toString();
        };
    }
}
