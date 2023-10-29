package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.redshift;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.redshift.RedshiftDataTypeConvertor.REDSHIFT_DECIMAL;

public class RedshiftCreateTableSqlBuilder {
    private List<Column> columns;
    private PrimaryKey primaryKey;
    private RedshiftDataTypeConvertor redshiftDataTypeConvertor;
    private String sourceCatalogName;

    public RedshiftCreateTableSqlBuilder(CatalogTable catalogTable) {
        this.columns = catalogTable.getTableSchema().getColumns();
        this.primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        this.redshiftDataTypeConvertor = new RedshiftDataTypeConvertor();
        this.sourceCatalogName = catalogTable.getCatalogName();
    }

    public String build(TablePath tablePath) {
        return build(tablePath, "");
    }

    public String build(TablePath tablePath, String fieldIde) {
        StringBuilder createTableSql = new StringBuilder();
        createTableSql
                .append(CatalogUtils.quoteIdentifier("CREATE TABLE ", fieldIde))
                .append(tablePath.getSchemaAndTableName("\""))
                .append(" (\n");

        List<String> columnSqls =
                columns.stream()
                        .map(
                                column ->
                                        CatalogUtils.quoteIdentifier(
                                                buildColumnSql(column), fieldIde))
                        .collect(Collectors.toList());

        createTableSql.append(String.join(",\n", columnSqls));
        createTableSql.append("\n);");

        List<String> commentSqls =
                columns.stream()
                        .filter(column -> StringUtils.isNotBlank(column.getComment()))
                        .map(
                                columns ->
                                        buildColumnCommentSql(
                                                columns,
                                                tablePath.getSchemaAndTableName("\""),
                                                fieldIde))
                        .collect(Collectors.toList());

        if (!commentSqls.isEmpty()) {
            createTableSql.append("\n");
            createTableSql.append(String.join(";\n", commentSqls)).append(";");
        }

        return createTableSql.toString();
    }

    private String buildColumnSql(Column column) {
        StringBuilder columnSql = new StringBuilder();
        columnSql.append("\"").append(column.getName()).append("\" ");
        String columnType =
                StringUtils.equals(sourceCatalogName, DatabaseIdentifier.REDSHIFT)
                                || StringUtils.equals(
                                        sourceCatalogName, DatabaseIdentifier.POSTGRESQL)
                        ? column.getSourceType()
                        : buildColumnType(column);
        columnSql.append(columnType);

        if (!column.isNullable()) {
            columnSql.append(" NOT NULL");
        }

        if (primaryKey != null && primaryKey.getColumnNames().contains(column.getName())) {
            columnSql.append(" PRIMARY KEY");
        }

        return columnSql.toString();
    }

    private String buildColumnType(Column column) {
        SqlType sqlType = column.getDataType().getSqlType();
        Long columnLength = column.getLongColumnLength();
        switch (sqlType) {
            case STRING:
                if (columnLength > 0 && columnLength < 10485760) {
                    return "varchar(" + columnLength + ")";
                } else {
                    return "text";
                }
            default:
                String type = redshiftDataTypeConvertor.toConnectorType(column.getDataType(), null);
                if (type.equals(REDSHIFT_DECIMAL)) {
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

    private String buildColumnCommentSql(Column column, String tableName, String fieldIde) {
        StringBuilder columnCommentSql = new StringBuilder();
        columnCommentSql
                .append(CatalogUtils.quoteIdentifier("COMMENT ON COLUMN ", fieldIde))
                .append(tableName)
                .append(".");
        columnCommentSql
                .append(CatalogUtils.quoteIdentifier(column.getName(), fieldIde, "\""))
                .append(CatalogUtils.quoteIdentifier(" IS '", fieldIde))
                .append(column.getComment())
                .append("'");
        return columnCommentSql.toString();
    }
}
