package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.db2;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static net.snowflake.client.jdbc.internal.microsoft.azure.storage.core.SR.BLOB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.db2.DB2DataTypeConvertor.DB2_BINARY;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.db2.DB2DataTypeConvertor.DB2_CHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.db2.DB2DataTypeConvertor.DB2_DECIMAL;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.db2.DB2DataTypeConvertor.DB2_LONG_VARCHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.db2.DB2DataTypeConvertor.DB2_VARBINARY;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.db2.DB2DataTypeConvertor.DB2_VARCHAR;

public class DB2CreateTableSqlBuilder {

    private List<Column> columns;
    private PrimaryKey primaryKey;
    private DB2DataTypeConvertor db2DataTypeConvertor;
    private String sourceCatalogName;
    private String fieldIde;
    private List<ConstraintKey> constraintKeys;
    public Boolean isHaveConstraintKey = false;

    public DB2CreateTableSqlBuilder(CatalogTable catalogTable) {
        this.columns = catalogTable.getTableSchema().getColumns();
        this.primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        this.db2DataTypeConvertor = new DB2DataTypeConvertor();
        this.sourceCatalogName = catalogTable.getCatalogName();
        this.fieldIde = catalogTable.getOptions().get("fieldIde");
        constraintKeys = catalogTable.getTableSchema().getConstraintKeys();
    }

    public String build(TablePath tablePath) {
        StringBuilder createTableSql = new StringBuilder();
        createTableSql
                .append(CatalogUtils.quoteIdentifier("CREATE TABLE IF NOT EXISTS ", fieldIde))
                .append(tablePath.getSchemaAndTableName("\""))
                .append(" (\n");

        List<String> columnSqls =
                columns.stream()
                        .map(
                                column ->
                                        CatalogUtils.quoteIdentifier(
                                                buildColumnSql(column), fieldIde))
                        .collect(Collectors.toList());

        if (CollectionUtils.isNotEmpty(constraintKeys)) {
            for (ConstraintKey constraintKey : constraintKeys) {
                if (StringUtils.isBlank(constraintKey.getConstraintName())
                        || (primaryKey != null
                                && StringUtils.equals(
                                        primaryKey.getPrimaryKey(),
                                        constraintKey.getConstraintName()))) {
                    continue;
                }
                String constraintKeySql = buildConstraintKeySql(constraintKey);
                if (StringUtils.isNotEmpty(constraintKeySql)) {
                    columnSqls.add("\t" + constraintKeySql);
                    isHaveConstraintKey = true;
                }
            }
        }

        createTableSql.append(String.join(",\n", columnSqls));
        createTableSql.append("\n);");

        List<String> commentSqls =
                columns.stream()
                        .filter(column -> StringUtils.isNotBlank(column.getComment()))
                        .map(
                                columns ->
                                        buildColumnCommentSql(
                                                columns, tablePath.getSchemaAndTableName("\"")))
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

        // For simplicity, assume the column type in SeaTunnelDataType is the same as in DB2
        String columnType =
                sourceCatalogName.equals(DatabaseIdentifier.DB_2)
                        ? column.getSourceType()
                        : buildColumnType(column);
        columnSql.append(columnType);

        // Add NOT NULL if column is not nullable
        if (!column.isNullable()) {
            columnSql.append(" NOT NULL");
        }

        // Add primary key directly after the column if it is a primary key
        if (primaryKey != null && primaryKey.getColumnNames().contains(column.getName())) {
            columnSql.append(" PRIMARY KEY");
        }

        return columnSql.toString();
    }

    private String buildColumnType(Column column) {
        SqlType sqlType = column.getDataType().getSqlType();
        Long columnLength = column.getLongColumnLength();
        Long bitLength = column.getBitLen();
        switch (sqlType) {
            case BYTES:
                if (bitLength != null && bitLength > 0 && bitLength <= 255) {
                    return DB2_BINARY + "(" + bitLength + ")";
                } else if (bitLength != null && bitLength > 255 && bitLength <= 32672) {
                    return DB2_VARBINARY + "(" + bitLength + ")";
                } else {
                    return BLOB;
                }
            case STRING:
                if (columnLength != null && columnLength > 0 && columnLength <= 255) {
                    return DB2_CHAR + "(" + columnLength + ")";
                } else if (columnLength != null && columnLength > 0 && columnLength <= 32672) {
                    return DB2_VARCHAR + "(" + columnLength + ")";
                } else {
                    return DB2_LONG_VARCHAR;
                }
            default:
                String type = db2DataTypeConvertor.toConnectorType(column.getDataType(), null);
                if (type.equals(DB2_DECIMAL)) {
                    DecimalType decimalType = (DecimalType) column.getDataType();
                    return "DECIMAL("
                            + decimalType.getPrecision()
                            + ","
                            + decimalType.getScale()
                            + ")";
                }
                return type;
        }
    }

    private String buildColumnCommentSql(Column column, String tableName) {
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

    private String buildConstraintKeySql(ConstraintKey constraintKey) {
        ConstraintKey.ConstraintType constraintType = constraintKey.getConstraintType();
        String randomSuffix = UUID.randomUUID().toString().replace("-", "").substring(0, 4);

        String constraintName = constraintKey.getConstraintName();
        if (constraintName.length() > 25) {
            constraintName = constraintName.substring(0, 25);
        }
        String indexColumns =
                constraintKey.getColumnNames().stream()
                        .map(
                                constraintKeyColumn ->
                                        String.format(
                                                "\"%s\"",
                                                CatalogUtils.getFieldIde(
                                                        constraintKeyColumn.getColumnName(),
                                                        fieldIde)))
                        .collect(Collectors.joining(", "));

        String keyName = null;
        switch (constraintType) {
            case INDEX_KEY:
                keyName = "KEY";
                break;
            case UNIQUE_KEY:
                keyName = "UNIQUE";
                break;
            case FOREIGN_KEY:
                keyName = "FOREIGN KEY";
                // todo:
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported constraint type: " + constraintType);
        }

        if (StringUtils.equals(keyName, "UNIQUE")) {
            isHaveConstraintKey = true;
            return "CONSTRAINT "
                    + constraintName
                    + "_"
                    + randomSuffix
                    + " UNIQUE ("
                    + indexColumns
                    + ")";
        }
        // todo KEY AND FOREIGN_KEY
        return null;
    }
}
