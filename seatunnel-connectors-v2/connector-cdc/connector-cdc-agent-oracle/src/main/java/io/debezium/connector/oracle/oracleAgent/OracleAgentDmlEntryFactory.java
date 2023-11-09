package io.debezium.connector.oracle.oracleAgent;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import org.whaleops.whaletunnel.oracleagent.sdk.model.OracleDeleteOperation;
import org.whaleops.whaletunnel.oracleagent.sdk.model.OracleInsertOperation;
import org.whaleops.whaletunnel.oracleagent.sdk.model.OracleOperation;
import org.whaleops.whaletunnel.oracleagent.sdk.model.OracleQmiOperation;
import org.whaleops.whaletunnel.oracleagent.sdk.model.OracleUpdateOperation;

import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.ValueConverter;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OracleAgentDmlEntryFactory {

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                    .optionalStart()
                    .appendPattern(".")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
                    .optionalEnd()
                    .toFormatter();

    private static final DateTimeFormatter DATE_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern("yyyy-MM-dd")
                    .optionalStart()
                    .appendPattern(".")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
                    .optionalEnd()
                    .optionalStart()
                    .appendPattern(" ")
                    .optionalEnd()
                    .appendOffset("+HH:MM", "")
                    .toFormatter();

    public static List<OracleAgentDmlEntry> transformOperation(
            OracleValueConverters oracleValueConverters, OracleOperation operation, Table table) {
        switch (operation.getType()) {
            case OracleInsertOperation.TYPE:
                return Collections.singletonList(
                        transformInsert(
                                oracleValueConverters, (OracleInsertOperation) operation, table));
            case OracleUpdateOperation.TYPE:
                return Collections.singletonList(
                        transformUpdate(
                                oracleValueConverters, (OracleUpdateOperation) operation, table));
            case OracleDeleteOperation.TYPE:
                return Collections.singletonList(
                        transformDelete(
                                oracleValueConverters, (OracleDeleteOperation) operation, table));
            case OracleQmiOperation.TYPE:
                return transformBatchInsert(
                        oracleValueConverters, (OracleQmiOperation) operation, table);
            default:
                throw new IllegalArgumentException(
                        "Unknown supported operation type: " + operation.getType());
        }
    }

    public static OracleAgentDmlEntry transformInsert(
            OracleValueConverters oracleValueConverters,
            OracleInsertOperation insertOperation,
            Table table) {
        Object[] newValues =
                getWholeColumnValues(oracleValueConverters, insertOperation.getInsertRow(), table);
        return OracleAgentDmlEntryImpl.forInsert(newValues);
    }

    public static OracleAgentDmlEntry transformUpdate(
            OracleValueConverters oracleValueConverters,
            OracleUpdateOperation updateOperation,
            Table table) {
        List<Column> columns = table.columns();
        Map<String, String> newRow = updateOperation.getUpdatedRow();
        Map<String, String> oldRow = updateOperation.getUpdateCondition();

        Object[] oldValues = getWholeColumnValues(oracleValueConverters, oldRow, table);
        Object[] newValues = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            if (newRow.containsKey(column.name())) {
                newValues[i] =
                        transformToOracleType(
                                oracleValueConverters, newRow.get(column.name()), column);
            } else {
                newValues[i] = oldValues[i];
            }
        }
        return OracleAgentDmlEntryImpl.forUpdate(newValues, oldValues);
    }

    public static OracleAgentDmlEntry transformDelete(
            OracleValueConverters oracleValueConverters,
            OracleDeleteOperation deleteOperation,
            Table table) {
        Object[] oldValues =
                getWholeColumnValues(oracleValueConverters, deleteOperation.getDeletedRow(), table);
        return OracleAgentDmlEntryImpl.forDelete(oldValues);
    }

    public static List<OracleAgentDmlEntry> transformBatchInsert(
            OracleValueConverters oracleValueConverters,
            OracleQmiOperation oracleQmiOperation,
            Table table) {
        return oracleQmiOperation.getInsertRows().stream()
                .map(
                        insertRow ->
                                OracleAgentDmlEntryImpl.forInsert(
                                        getWholeColumnValues(
                                                oracleValueConverters, insertRow, table)))
                .collect(Collectors.toList());
    }

    private static Object[] getWholeColumnValues(
            OracleValueConverters oracleValueConverters,
            Map<String, String> columnValues,
            Table table) {
        List<Column> columns = table.columns();
        Object[] objects = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            objects[i] =
                    transformToOracleType(
                            oracleValueConverters, columnValues.get(column.name()), column);
        }
        return objects;
    }

    private static Object transformToOracleType(
            OracleValueConverters oracleValueConverters, String value, Column column) {
        Object oracleValue = value;

        try {
            if (column.typeName().equals("DATE")) {
                oracleValue = LocalDate.from(DATE_FORMATTER.parse(value));
            }
        } catch (Exception ex) {
            // this is a tmp solution
            oracleValue = LocalDate.from(TIMESTAMP_FORMATTER.parse(value));
        }

        if (column.typeName().startsWith("TIMESTAMP")) {
            oracleValue = LocalDateTime.from(TIMESTAMP_FORMATTER.parse(value));
        }
        SchemaBuilder schemaBuilder = oracleValueConverters.schemaBuilder(column);
        if (schemaBuilder == null) {
            return oracleValue;
        }
        Schema schema = schemaBuilder.build();
        Field field = new Field(column.name(), 1, schema);
        final ValueConverter valueConverter = oracleValueConverters.converter(column, field);
        return valueConverter.convert(oracleValue);
    }
}
