package org.apache.seatunnel.connectors.dolphindb.utils;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DolphinDBSaveModeUtilTest {

    @Test
    void fillingCreateSql() {
        List<Column> columns = new ArrayList<>();

        columns.add(PhysicalColumn.of("id", BasicType.LONG_TYPE, null, true, null, ""));
        columns.add(PhysicalColumn.of("name", BasicType.STRING_TYPE, null, true, null, ""));
        columns.add(PhysicalColumn.of("age", BasicType.INT_TYPE, null, true, null, ""));
        columns.add(PhysicalColumn.of("gender", BasicType.BYTE_TYPE, null, true, null, ""));
        columns.add(PhysicalColumn.of("create_time", BasicType.LONG_TYPE, null, true, null, ""));

        String result =
                DolphinDBSaveModeUtil.fillingCreateSql(
                        "CREATE TABLE '${database}'.'${table_name}'\" (                                                                                                                                                   \n"
                                + "${rowtype_primary_key}  ,       \n"
                                + "create_time TIMESTAMP,  \n"
                                + "${rowtype_fields}"
                                + ")\n"
                                + "partitioned BY ${rowtype_primary_key} ;",
                        "dfs://whalescheduler",
                        "partition_table2",
                        TableSchema.builder()
                                .primaryKey(PrimaryKey.of("", Arrays.asList("id")))
                                .columns(columns)
                                .build());

        System.out.println(result);
    }
}
