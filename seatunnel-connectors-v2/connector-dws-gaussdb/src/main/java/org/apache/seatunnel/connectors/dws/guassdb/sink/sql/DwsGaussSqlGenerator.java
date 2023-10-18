package org.apache.seatunnel.connectors.dws.guassdb.sink.sql;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.dws.guassdb.catalog.DwsGaussDBDataTypeConvertor;
import org.apache.seatunnel.connectors.dws.guassdb.sink.config.DwsGaussDBSinkOption;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class DwsGaussSqlGenerator implements Serializable {

    private final CatalogTable catalogTable;
    private final DwsGaussDBSinkOption.FieldIdeEnum fieldIdeEnum;
    // todo: use primary key in catalog table
    private final String primaryKey;
    private final String schemaName;
    private final String templateTableName;

    private final String targetTableName;

    private final String delimiter = "|";

    private final DwsGaussDBDataTypeConvertor dwsGaussDBDataTypeConvertor;

    public DwsGaussSqlGenerator(
            String primaryKey,
            DwsGaussDBSinkOption.FieldIdeEnum fieldIdeEnum,
            CatalogTable catalogTable) {
        this.fieldIdeEnum = checkNotNull(fieldIdeEnum);
        if (StringUtils.isNotEmpty(primaryKey)) {
            this.primaryKey = getIDEString(primaryKey);
        } else {
            this.primaryKey = primaryKey;
        }
        this.catalogTable = catalogTable;
        this.schemaName =
                Optional.ofNullable(catalogTable.getTableId().getSchemaName())
                        .map(this::getIDEString)
                        .orElse("default");
        this.targetTableName = getIDEString(catalogTable.getTableId().getTableName());
        this.templateTableName = getIDEString("st_temporary_" + targetTableName);
        this.dwsGaussDBDataTypeConvertor = new DwsGaussDBDataTypeConvertor();
    }

    public String getTemporaryTableName() {
        return templateTableName;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public String getCopyInTemporaryTableSql() {
        return "COPY \""
                + schemaName
                + "\".\""
                + templateTableName
                + "\" FROM STDIN DELIMITER '"
                + delimiter
                + "'";
    }

    public String getCopyInTargetTableSql() {
        return "COPY \""
                + schemaName
                + "\".\""
                + targetTableName
                + "\" FROM STDIN DELIMITER '"
                + delimiter
                + "'";
    }

    public String getMergeInTargetTableSql(String snapshotId) {
        String sql =
                "INSERT INTO %s SELECT %s FROM %s WHERE st_snapshot_id = '%s' "
                        + "ON CONFLICT(%s) "
                        + "DO UPDATE SET %s;";

        // inject table
        String targetTable = "\"" + schemaName + "\".\"" + targetTableName + "\"";
        String temporaryTable = "\"" + schemaName + "\".\"" + templateTableName + "\"";

        List<String> updateColumns = new ArrayList<>();
        List<Column> columns = catalogTable.getTableSchema().getColumns();
        List<String> columnNames =
                columns.stream()
                        .map(column -> getIDEString(column.getName()))
                        .collect(Collectors.toList());
        for (String columnName : columnNames) {
            if (columnName.equals(primaryKey)) {
                // the primary key doesn't need to update
                continue;
            }
            updateColumns.add(columnName + "=" + "EXCLUDED." + columnName);
        }

        return String.format(
                sql,
                targetTable,
                columnNames.stream().collect(Collectors.joining(",")),
                temporaryTable,
                snapshotId,
                primaryKey,
                String.join(",", updateColumns));
    }

    public String getTemporaryRows(
            Collection<SeaTunnelRow> seaTunnelRows, boolean isDeleteRow, String snapshotId) {
        return seaTunnelRows.stream()
                .map(
                        seaTunnelRow ->
                                appendRowInTemporaryTable(seaTunnelRow, isDeleteRow, snapshotId))
                .collect(Collectors.joining("\n"));
    }

    public String getTargetTableRows(Collection<SeaTunnelRow> seaTunnelRows) {
        return seaTunnelRows.stream()
                .map(this::appendRowInTargetTable)
                .collect(Collectors.joining("\n"));
    }

    public String getDeleteTemporarySnapshotSql(String snapshotId) {
        return "DELETE FROM \""
                + schemaName
                + "\".\""
                + templateTableName
                + "\" WHERE st_snapshot_id = '"
                + snapshotId
                + "'";
    }

    public String getDeleteTargetTableSql() {
        return "DELETE FROM \"" + schemaName + "\".\"" + targetTableName + "\"";
    }

    public String getDeleteTemporaryTableSql() {
        return "DELETE FROM \"" + schemaName + "\".\"" + templateTableName + "\"";
    }

    public String getDropTemporaryTableSql() {
        return "DROP TABLE IF EXISTS \"" + schemaName + "\".\"" + templateTableName + "\"";
    }

    public String getDropTargetTableSql() {
        return "DROP TABLE IF EXISTS \"" + schemaName + "\".\"" + targetTableName + "\"";
    }

    public String getQuertTargetTableDataCountSql() {
        return "SELECT COUNT(*) FROM \"" + schemaName + "\".\"" + targetTableName + "\"";
    }

    public String getDeleteRowsInTargetTableSql(String currentSnapshotId) {
        // todo: only support one primary key
        return "DELETE FROM \""
                + schemaName
                + "\".\""
                + targetTableName
                + "\" WHERE "
                + primaryKey
                + " IN (SELECT "
                + primaryKey
                + " FROM \""
                + schemaName
                + "\".\""
                + templateTableName
                + "\" WHERE st_snapshot_id = '"
                + currentSnapshotId
                + "' AND st_is_deleted = true)";
    }

    public String getDeleteRowsInTemporaryTableSql(String currentSnapshotId) {
        return "DELETE FROM \""
                + schemaName
                + "\".\""
                + templateTableName
                + "\" WHERE st_snapshot_id = '"
                + currentSnapshotId
                + "' AND st_is_deleted = true";
    }

    public String getCreateTemporaryTableSql() {
        StringBuilder createTemporaryTableSql = new StringBuilder();

        createTemporaryTableSql
                .append("CREATE TABLE IF NOT EXISTS ")
                .append("\"" + schemaName + "\".\"" + templateTableName + "\"")
                .append(" (\n");

        List<String> columnSqls =
                catalogTable.getTableSchema().getColumns().stream()
                        .map(this::buildColumnSql)
                        .collect(Collectors.toList());
        // add snapshot_id and is_deleted column
        columnSqls.add("\"" + getIDEString("st_snapshot_id") + "\" varchar(255)");
        columnSqls.add("\"" + getIDEString("st_is_deleted") + "\" boolean");
        createTemporaryTableSql.append(String.join(",\n", columnSqls));
        createTemporaryTableSql.append("\n);");
        // add index for snapshot_id
        columnSqls.add("INDEX (" + getIDEString("st_snapshot_id") + ")");

        return createTemporaryTableSql.toString();
    }

    public String getCreateTargetTableSql() {

        StringBuilder createTemporaryTableSql = new StringBuilder();

        createTemporaryTableSql
                .append("CREATE TABLE IF NOT EXISTS ")
                .append(
                        getIDEString(
                                catalogTable
                                        .getTableId()
                                        .toTablePath()
                                        .getSchemaAndTableName("\"")))
                .append(" (\n");

        List<String> columnSqls =
                catalogTable.getTableSchema().getColumns().stream()
                        .map(this::buildColumnSql)
                        .collect(Collectors.toList());
        createTemporaryTableSql.append(String.join(",\n", columnSqls));
        createTemporaryTableSql.append("\n);");

        return createTemporaryTableSql.toString();
    }

    private String appendRowInTemporaryTable(
            SeaTunnelRow seaTunnelRow, boolean isDeleted, String snapshotId) {
        StringBuilder stringBuilder = new StringBuilder();
        Object[] fields = seaTunnelRow.getFields();
        for (int i = 0; i < fields.length; i++) {
            if (seaTunnelRow.getField(i) == null) {
                // use '' represent null
            } else {
                stringBuilder.append(seaTunnelRow.getField(i));
            }
            stringBuilder.append(delimiter);
        }
        // todo: If the schema changed, we need to make sure the snapshotId and isDeleted flag is
        // the last two column
        stringBuilder.append(snapshotId);
        stringBuilder.append(delimiter);
        stringBuilder.append(isDeleted);
        return stringBuilder.toString();
    }

    private String appendRowInTargetTable(SeaTunnelRow seaTunnelRow) {
        Object[] fields = seaTunnelRow.getFields();
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            if (seaTunnelRow.getField(i) == null) {
                // use '' represent null
            } else {
                stringBuilder.append(seaTunnelRow.getField(i));
            }
            if (i != fields.length - 1) {
                stringBuilder.append(delimiter);
            }
        }
        return stringBuilder.toString();
    }

    private String buildColumnSql(Column column) {
        StringBuilder columnSql = new StringBuilder();
        columnSql
                .append("\"")
                .append(getIDEString(column.getName()))
                .append("\" ")
                .append(buildColumnType(column));

        // Add NOT NULL if column is not nullable
        if (!column.isNullable()) {
            columnSql.append(" NOT NULL");
        }

        // Add primary key directly after the column if it is a primary key
        if (StringUtils.isNotEmpty(primaryKey)) {
            if (primaryKey.equals(getIDEString(column.getName()))) {
                columnSql.append(" PRIMARY KEY");
            }
        }

        return columnSql.toString();
    }

    private String buildColumnType(Column column) {
        SqlType sqlType = column.getDataType().getSqlType();
        Long columnLength = column.getLongColumnLength();
        switch (sqlType) {
            case BYTES:
                return "bytea";
            case STRING:
                if (columnLength > 0 && columnLength < 10485760) {
                    return "varchar(" + columnLength + ")";
                } else {
                    return "text";
                }
            default:
                String type =
                        dwsGaussDBDataTypeConvertor.toConnectorType(column.getDataType(), null);
                if (type.equals("numeric")) {
                    DecimalType decimalType = (DecimalType) column.getDataType();
                    return "numeric("
                            + decimalType.getPrecision()
                            + ","
                            + decimalType.getScale()
                            + ")";
                }
                return type;
        }
    }

    private String getIDEString(String originString) {
        if (originString == null) {
            return originString;
        }
        switch (fieldIdeEnum) {
            case ORIGINAL:
                return originString;
            case LOWERCASE:
                return originString.toLowerCase();
            case UPPERCASE:
                return originString.toUpperCase();
            default:
                return originString;
        }
    }
}
