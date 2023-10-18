package org.apache.seatunnel.connectors.dws.guassdb.sink.sql;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DwsGaussSqlGeneratorTest {

    private static DwsGaussSqlGenerator dwsGaussSqlGenerator;

    @BeforeAll
    public static void before() {
        List<Column> columns = new ArrayList<>();
        columns.add(PhysicalColumn.of("id", BasicType.INT_TYPE, 0, false, null, null));
        columns.add(PhysicalColumn.of("name", BasicType.STRING_TYPE, 0, false, null, null));
        columns.add(PhysicalColumn.of("age", BasicType.INT_TYPE, 0, false, null, null));
        columns.add(
                PhysicalColumn.of(
                        "create_time", LocalTimeType.LOCAL_DATE_TIME_TYPE, 0, false, null, null));

        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(columns)
                        .primaryKey(PrimaryKey.of("id", Lists.newArrayList("id")))
                        .constraintKey(
                                ConstraintKey.of(
                                        ConstraintKey.ConstraintType.INDEX_KEY,
                                        "id",
                                        Lists.newArrayList(
                                                ConstraintKey.ConstraintKeyColumn.of(
                                                        "id", ConstraintKey.ColumnSortType.ASC))))
                        .build();

        TableIdentifier tableIdentifier =
                TableIdentifier.of("Dws-GaussDB", "database", "public", "t_st_users");
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableIdentifier, tableSchema, new HashMap<>(), new ArrayList<>(), "");
        dwsGaussSqlGenerator =
                new DwsGaussSqlGenerator(
                        "id", DwsGaussDBSinkOption.FieldIdeEnum.ORIGINAL, catalogTable);
    }

    @Test
    void getTemporaryTableName() {
        String temporaryTableName = dwsGaussSqlGenerator.getTemporaryTableName();
        Assertions.assertEquals("st_temporary_t_st_users", temporaryTableName);
    }

    @Test
    void getTargetTableName() {
        String targetTableName = dwsGaussSqlGenerator.getTargetTableName();
        Assertions.assertEquals("t_st_users", targetTableName);
    }

    @Test
    void getCopyInTemporaryTableSql() {
        String copyInTemporaryTableSql = dwsGaussSqlGenerator.getCopyInTemporaryTableSql();
        Assertions.assertEquals(
                "COPY \"public\".\"st_temporary_t_st_users\" FROM STDIN DELIMITER '|'",
                copyInTemporaryTableSql);
    }

    @Test
    void getCopyInTargetTableSql() {
        String copyInTargetTableSql = dwsGaussSqlGenerator.getCopyInTargetTableSql();
        Assertions.assertEquals(
                "COPY \"public\".\"t_st_users\" FROM STDIN DELIMITER '|'", copyInTargetTableSql);
    }

    @Test
    void getTemporaryRows() {
        List<SeaTunnelRow> seaTunnelRows = new ArrayList<>();
        Object[] fields = new Object[4];
        fields[0] = 1;
        fields[1] = "tom";
        fields[2] = 18;
        fields[3] = LocalDateTime.of(2023, 9, 7, 10, 10, 10);
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);
        String deleteRows =
                dwsGaussSqlGenerator.getTemporaryRows(
                        Lists.newArrayList(seaTunnelRow),
                        true,
                        "be9a636f-7e24-474b-84ae-62bc7ea3f914");
        Assertions.assertEquals(
                "1|tom|18|2023-09-07T10:10:10|be9a636f-7e24-474b-84ae-62bc7ea3f914|true",
                deleteRows);

        String upsertRows =
                dwsGaussSqlGenerator.getTemporaryRows(
                        Lists.newArrayList(seaTunnelRow),
                        false,
                        "be9a636f-7e24-474b-84ae-62bc7ea3f914");
        Assertions.assertEquals(
                "1|tom|18|2023-09-07T10:10:10|be9a636f-7e24-474b-84ae-62bc7ea3f914|false",
                upsertRows);
    }

    @Test
    void getTargetTableRows() {
        Object[] fields = new Object[4];
        fields[0] = 1;
        fields[1] = "tom";
        fields[2] = 18;
        fields[3] = LocalDateTime.of(2023, 9, 7, 10, 10, 10);
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);

        String targetTableRows =
                dwsGaussSqlGenerator.getTargetTableRows(Lists.newArrayList(seaTunnelRow));
        Assertions.assertEquals("1|tom|18|2023-09-07T10:10:10", targetTableRows);

        fields = new Object[4];
        fields[0] = 1;
        fields[1] = "tom";
        fields[2] = null;
        fields[3] = LocalDateTime.of(2023, 9, 7, 10, 10, 10);
        seaTunnelRow = new SeaTunnelRow(fields);

        targetTableRows = dwsGaussSqlGenerator.getTargetTableRows(Lists.newArrayList(seaTunnelRow));
        Assertions.assertEquals("1|tom||2023-09-07T10:10:10", targetTableRows);
    }

    @Test
    void getDeleteTemporarySnapshotSql() {
        String deleteTemporarySnapshotSql =
                dwsGaussSqlGenerator.getDeleteTemporarySnapshotSql(
                        "be9a636f-7e24-474b-84ae-62bc7ea3f914");
        Assertions.assertEquals(
                "DELETE FROM \"public\".\"st_temporary_t_st_users WHERE st_snapshot_id = 'be9a636f-7e24-474b-84ae-62bc7ea3f914'",
                deleteTemporarySnapshotSql);
    }

    @Test
    void getDeleteTargetTableSql() {
        String deleteTargetTableSql = dwsGaussSqlGenerator.getDeleteTargetTableSql();
        Assertions.assertEquals("DELETE FROM \"public\".\"t_st_users\"", deleteTargetTableSql);
    }

    @Test
    void getDropTemporaryTableSql() {
        String dropTemporaryTableSql = dwsGaussSqlGenerator.getDropTemporaryTableSql();
        Assertions.assertEquals(
                "DROP TABLE IF EXISTS \"public\".\"st_temporary_t_st_users\"",
                dropTemporaryTableSql);
    }

    @Test
    void getCreateTemporaryTableSql() {
        String createTemporaryTableSql = dwsGaussSqlGenerator.getCreateTemporaryTableSql();
        Assertions.assertEquals(
                "CREATE TABLE IF NOT EXISTS \"public\".\"t_st_users\" (\n"
                        + "\"id\" int4 NOT NULL PRIMARY KEY,\n"
                        + "\"name\" text NOT NULL,\n"
                        + "\"age\" int4 NOT NULL,\n"
                        + "\"create_time\" timestamp NOT NULL,\n"
                        + "\"st_snapshot_id\" varchar(255),\n"
                        + "\"st_is_deleted\" boolean\n"
                        + ");",
                createTemporaryTableSql);
    }

    @Test
    void testGetTemporaryTableName() {
        Assertions.assertEquals(
                "st_temporary_t_st_users", dwsGaussSqlGenerator.getTemporaryTableName());
    }

    @Test
    void testGetTargetTableName() {
        Assertions.assertEquals("t_st_users", dwsGaussSqlGenerator.getTargetTableName());
    }

    @Test
    void testGetCopyInTemporaryTableSql() {
        Assertions.assertEquals(
                "COPY \"public\".\"st_temporary_t_st_users\" FROM STDIN DELIMITER '|'",
                dwsGaussSqlGenerator.getCopyInTemporaryTableSql());
    }

    @Test
    void testGetCopyInTargetTableSql() {
        Assertions.assertEquals(
                "COPY \"public\".\"t_st_users\" FROM STDIN DELIMITER '|'",
                dwsGaussSqlGenerator.getCopyInTargetTableSql());
    }

    @Test
    void getMergeInTargetTableSql() {
        String mergeInTargetTableSql =
                dwsGaussSqlGenerator.getMergeInTargetTableSql(
                        "be9a636f-7e24-474b-84ae-62bc7ea3f914");
        Assertions.assertEquals(
                "INSERT INTO \"public\".\"t_st_users\" SELECT id,name,age,create_time FROM \"public\".\"st_temporary_t_st_users\" WHERE st_current_snapshot_id = be9a636f-7e24-474b-84ae-62bc7ea3f914 ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,age=EXCLUDED.age,create_time=EXCLUDED.create_time;",
                mergeInTargetTableSql);
    }

    @Test
    void getDeleteRowsInTargetTableSql() {
        String deleteRowsInTargetTableSql =
                dwsGaussSqlGenerator.getDeleteRowsInTargetTableSql(
                        "be9a636f-7e24-474b-84ae-62bc7ea3f914");
        Assertions.assertEquals(
                "DELETE FROM \"public\".\"t_st_users\" WHERE id IN (SELECT id FROM \"public\".\"st_temporary_t_st_users\" WHERE st_snapshot_id = 'be9a636f-7e24-474b-84ae-62bc7ea3f914' AND st_is_deleted = true)",
                deleteRowsInTargetTableSql);
    }

    @Test
    void getDeleteRowsInTemporaryTableSql() {
        String deleteRowsInTemporaryTableSql =
                dwsGaussSqlGenerator.getDeleteRowsInTemporaryTableSql(
                        "be9a636f-7e24-474b-84ae-62bc7ea3f914");
        Assertions.assertEquals(
                "DELETE FROM \"public\".\"st_temporary_t_st_users\" WHERE st_snapshot_id = 'be9a636f-7e24-474b-84ae-62bc7ea3f914' AND st_is_deleted = true",
                deleteRowsInTemporaryTableSql);
    }
}
