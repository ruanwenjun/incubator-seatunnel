package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class OracleCreateTableSqlBuilder {

    private List<Column> columns;
    private PrimaryKey primaryKey;
    private OracleDataTypeConvertor oracleDataTypeConvertor;
    private String sourceCatalogName;

    public OracleCreateTableSqlBuilder(CatalogTable catalogTable) {
        this.columns = catalogTable.getTableSchema().getColumns();
        this.primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        this.oracleDataTypeConvertor = new OracleDataTypeConvertor();
        this.sourceCatalogName = catalogTable.getCatalogName();
    }

    public String build(TablePath tablePath) {
        return build(tablePath, "");
    }

    public String build(TablePath tablePath, String fieldIde) {
        StringBuilder createTableSql = new StringBuilder();
        createTableSql
                .append("CREATE TABLE ")
                .append(tablePath.getSchemaAndTableName("\""))
                .append(" (\n");

        List<String> columnSqls =
                columns.stream()
                        .map(column -> CatalogUtils.getFieldIde(buildColumnSql(column), fieldIde))
                        .collect(Collectors.toList());

        // Add primary key directly in the create table statement
        if (primaryKey != null
                && primaryKey.getColumnNames() != null
                && primaryKey.getColumnNames().size() > 0) {
            columnSqls.add(buildPrimaryKeySql(primaryKey, fieldIde));
        }

        createTableSql.append(String.join(",\n", columnSqls));
        createTableSql.append("\n)");

        List<String> commentSqls =
                columns.stream()
                        .filter(column -> StringUtils.isNotBlank(column.getComment()))
                        .map(
                                column ->
                                        buildColumnCommentSql(
                                                column,
                                                tablePath.getSchemaAndTableName("\""),
                                                fieldIde))
                        .collect(Collectors.toList());

        if (!commentSqls.isEmpty()) {
            createTableSql.append(";\n");
            createTableSql.append(String.join(";\n", commentSqls));
        }

        return createTableSql.toString();
    }

    private String buildColumnSql(Column column) {
        StringBuilder columnSql = new StringBuilder();
        columnSql.append("\"").append(column.getName()).append("\" ");

        String columnType =
                sourceCatalogName.equals("oracle")
                        ? column.getSourceType()
                        : buildColumnType(column);
        columnSql.append(columnType);

        if (!column.isNullable()) {
            columnSql.append(" NOT NULL");
        }

        //        if (column.getDefaultValue() != null) {
        //            columnSql.append(" DEFAULT
        // '").append(column.getDefaultValue().toString()).append("'");
        //        }

        return columnSql.toString();
    }

    private String buildColumnType(Column column) {
        SqlType sqlType = column.getDataType().getSqlType();
        Long columnLength = column.getLongColumnLength();
        Long bitLen = column.getBitLen();
        switch (sqlType) {
            case BYTES:
                if (bitLen < 0 || bitLen > 2000) {
                    return "BLOB";
                } else {
                    return "RAW(" + bitLen + ")";
                }
            case STRING:
                if (columnLength > 0 && columnLength < 4000) {
                    return "VARCHAR2(" + columnLength + " CHAR)";
                } else {
                    return "CLOB";
                }
            default:
                String type = oracleDataTypeConvertor.toConnectorType(column.getDataType(), null);
                if (type.equals("NUMBER")) {
                    if (column.getDataType() instanceof DecimalType) {
                        DecimalType decimalType = (DecimalType) column.getDataType();
                        return "NUMBER("
                                + decimalType.getPrecision()
                                + ","
                                + decimalType.getScale()
                                + ")";
                    } else {
                        return "NUMBER";
                    }
                }
                return type;
        }
    }

    private String buildPrimaryKeySql(PrimaryKey primaryKey, String fieldIde) {
        String randomSuffix = UUID.randomUUID().toString().replace("-", "").substring(0, 4);
        //        String columnNamesString = String.join(", ", primaryKey.getColumnNames());
        String columnNamesString =
                primaryKey.getColumnNames().stream()
                        .map(columnName -> "\"" + columnName + "\"")
                        .collect(Collectors.joining(", "));

        // In Oracle database, the maximum length for an identifier is 30 characters.
        String primaryKeyStr = primaryKey.getPrimaryKey();
        if (primaryKeyStr.length() > 25) {
            primaryKeyStr = primaryKeyStr.substring(0, 25);
        }

        return CatalogUtils.getFieldIde(
                "CONSTRAINT "
                        + primaryKeyStr
                        + "_"
                        + randomSuffix
                        + " PRIMARY KEY ("
                        + columnNamesString
                        + ")",
                fieldIde);
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
