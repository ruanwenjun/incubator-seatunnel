/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.redshift.sink;

import org.apache.seatunnel.shade.com.google.common.base.Strings;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConf;
import org.apache.seatunnel.connectors.seatunnel.redshift.datatype.ToRedshiftTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.redshift.exception.S3RedshiftConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.redshift.exception.S3RedshiftConnectorException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Slf4j
@Getter
@ToString
public class S3RedshiftSQLGenerator implements Serializable {
    private final S3RedshiftConf conf;
    private final CatalogTable table;
    private final SeaTunnelRowType rowType;
    private final CatalogTable temporaryTable;
    private final SeaTunnelRowType temporaryRowType;
    private final String createTableSQL;
    private final String cleanTableSql;
    private final String dropTableSql;
    private final String isExistTableSql;
    private final String isExistDataSql;
    private Pair<String[], String> sortKeyValueQuerySql;
    private String copyS3FileToTableSql;
    private String createTemporaryTableSQL;
    private String copyS3FileToTemporaryTableSql;
    private String cleanTemporaryTableSql;
    private String dropTemporaryTableSql;
    private String queryDeleteRowCountSql;
    private String deleteTargetTableRowSql;
    private String cleanDeleteRowCountSql;

    public S3RedshiftSQLGenerator(S3RedshiftConf conf, CatalogTable table) {
        this(conf, table, table.getTableSchema().toPhysicalRowDataType());
    }

    public S3RedshiftSQLGenerator(S3RedshiftConf conf, SeaTunnelRowType rowType) {
        this(conf, null, rowType);
    }

    private S3RedshiftSQLGenerator(
            S3RedshiftConf conf, CatalogTable table, SeaTunnelRowType rowType) {
        this.conf = conf;

        if (table != null) {
            TableSchemaEnhancer schemaEnhancer = new TableSchemaEnhancer(table);
            this.table = schemaEnhancer.getPrimitiveTable();
            this.temporaryTable = schemaEnhancer.getEnhanceTable();
            this.rowType = schemaEnhancer.getPrimitiveRowType();
            this.temporaryRowType = schemaEnhancer.getEnhanceRowType();
        } else {
            TableSchemaEnhancer schemaEnhancer = new TableSchemaEnhancer(rowType);
            this.table = null;
            this.temporaryTable = null;
            this.rowType = schemaEnhancer.getPrimitiveRowType();
            this.temporaryRowType = schemaEnhancer.getEnhanceRowType();
        }

        this.createTableSQL = generateCreateTableSQL();
        this.cleanTableSql = generateCleanTableSql();
        this.isExistTableSql = generateIsExistTableSql();
        this.isExistDataSql = generateIsExistDataSql();
        this.dropTableSql = generateDropTableSQL();
        this.copyS3FileToTableSql = generateCopyS3FileToTargetTableSql();

        if (conf.notAppendOnlyMode()) {
            this.sortKeyValueQuerySql = generateTemporaryTableSortKeyValueQuerySql();
            this.createTemporaryTableSQL = generateCreateTemporaryTableSQL();
            this.copyS3FileToTemporaryTableSql = generateCopyS3FileToTemporaryTableSql();
            this.cleanTemporaryTableSql = generateCleanTemporaryTableSql();
            this.dropTemporaryTableSql = generateDropTemporaryTableSql();
            this.queryDeleteRowCountSql = generateQueryDeleteRowCountSql();
            this.deleteTargetTableRowSql = generateDeleteTargetTableRowSql();
            this.cleanDeleteRowCountSql = generateCleanDeleteRowSql();
        }
    }

    private String generateCreateTableSQL() {
        String columnDefinition;
        if (table != null) {
            columnDefinition = generateColumnDefinition(table);
        } else {
            columnDefinition =
                    generateColumnDefinition(rowType, conf.getRedshiftTablePrimaryKeys());
        }
        String ddl =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.%s (%s)",
                        conf.getSchema(), conf.getRedshiftTable(), columnDefinition);
        List<String> sortKey = getTableSortKey();
        if (!sortKey.isEmpty()) {
            ddl = ddl + String.format(" SORTKEY(%s)", String.join(",", sortKey));
        }
        return ddl;
    }

    private String generateDropTableSQL() {
        return String.format(
                "DROP TABLE IF EXISTS %s.%s ;", conf.getSchema(), conf.getRedshiftTable());
    }

    private String generateCreateTemporaryTableSQL() {
        String columnDefinition;
        if (temporaryTable != null) {
            columnDefinition = generateColumnDefinition(temporaryTable);
        } else {
            columnDefinition =
                    generateColumnDefinition(temporaryRowType, conf.getRedshiftTablePrimaryKeys());
        }

        String ddl =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s.%s (%s)",
                        conf.getSchema(), conf.getTemporaryTableName(), columnDefinition);
        List<String> sortKey = getTemporaryTableSortKey();
        if (!sortKey.isEmpty()) {
            ddl = ddl + String.format(" SORTKEY(%s)", String.join(",", sortKey));
        }
        return ddl;
    }

    private String generateCopyS3FileToTemporaryTableSql() {
        return generateCopyS3FileToTableSql(conf.getTemporaryTableName(), temporaryRowType);
    }

    private String generateCopyS3FileToTargetTableSql() {
        return generateCopyS3FileToTableSql(conf.getRedshiftTable(), rowType);
    }

    private String generateCopyS3FileToTableSql(String table, SeaTunnelRowType rowType) {
        String bucket = getBucket(conf.getS3Bucket());
        if (bucket.endsWith("/")) {
            bucket = bucket.substring(0, bucket.lastIndexOf("/"));
        }
        String columns = String.join(",", rowType.getFieldNames());
        if (!Strings.isNullOrEmpty(conf.getAccessKey())
                && !Strings.isNullOrEmpty(conf.getSecretKey())) {
            return String.format(
                    "COPY %s.%s(%s) FROM '%s/${path}' ACCESS_KEY_ID '%s' SECRET_ACCESS_KEY '%s' FILLRECORD FORMAT ORC SERIALIZETOJSON",
                    conf.getSchema(),
                    table,
                    columns,
                    bucket,
                    conf.getAccessKey(),
                    conf.getSecretKey());
        }
        if (!Strings.isNullOrEmpty(conf.getRedshiftS3IamRole())) {
            return String.format(
                    "COPY %s.%s(%s) FROM '%s/${path}' IAM_ROLE '%s' FILLRECORD FORMAT ORC SERIALIZETOJSON",
                    conf.getSchema(), table, columns, bucket, conf.getRedshiftS3IamRole());
        }
        throw new IllegalArgumentException("Either accessKey/secretKey or iamRole must be set");
    }

    private String generateCleanTemporaryTableSql() {
        return String.format(
                "TRUNCATE TABLE %s.%s;", conf.getSchema(), conf.getTemporaryTableName());
    }

    private String generateCleanTableSql() {
        return String.format("TRUNCATE TABLE %s.%s;", conf.getSchema(), conf.getRedshiftTable());
    }

    private String generateIsExistTableSql() {
        return String.format(
                "SELECT * FROM information_schema.tables where table_schema = '%s' and  table_name = '%s';",
                conf.getSchema(), conf.getRedshiftTable().toLowerCase());
    }

    private String generateIsExistDataSql() {
        return String.format(
                "select 1 from %s.%s limit 1;", conf.getSchema(), conf.getRedshiftTable());
    }

    private String generateDropTemporaryTableSql() {
        return String.format(
                "DROP TABLE IF EXISTS %s.%s", conf.getSchema(), conf.getTemporaryTableName());
    }

    private String generateQueryDeleteRowCountSql() {
        return String.format(
                "SELECT COUNT(1) FROM %s.%s WHERE %s = true",
                conf.getSchema(),
                conf.getTemporaryTableName(),
                TableSchemaEnhancer.METADATA_DELETE_FIELD);
    }

    private String generateDeleteTargetTableRowSql() {
        String targetTable = conf.getSchema() + "." + conf.getRedshiftTable();
        String temporaryTable = conf.getSchema() + "." + conf.getTemporaryTableName();
        String conditionClause =
                getPrimaryKeys().stream()
                        .map(
                                field ->
                                        String.format(
                                                "%s.%s = %s.%s",
                                                targetTable, field, temporaryTable, field))
                        .collect(Collectors.joining(" AND "));
        String deleteRowFilter =
                String.format(
                        "%s.%s = true", temporaryTable, TableSchemaEnhancer.METADATA_DELETE_FIELD);
        return String.format(
                "DELETE FROM %s USING %s WHERE %s AND %s",
                targetTable, temporaryTable, deleteRowFilter, conditionClause);
    }

    public String generateCleanDeleteRowSql() {
        return String.format(
                "DELETE FROM %s.%s WHERE %s = true",
                conf.getSchema(),
                conf.getTemporaryTableName(),
                TableSchemaEnhancer.METADATA_DELETE_FIELD);
    }

    public String generateMergeSql(
            @NonNull Map<String, ImmutablePair<Object, Object>> sortKeyValues) {
        String conditionClause =
                conf.getRedshiftTablePrimaryKeys().stream()
                        .map(
                                field ->
                                        String.format(
                                                "%s.%s = source.%s",
                                                conf.getRedshiftTable(), field, field))
                        .collect(Collectors.joining(" AND "));
        String sortKeyCondition =
                sortKeyValues.entrySet().stream()
                        .map(
                                entry -> {
                                    String sortColumn = entry.getKey();
                                    SeaTunnelDataType<?> fieldType =
                                            rowType.getFieldType(rowType.indexOf(sortColumn));
                                    switch (fieldType.getSqlType()) {
                                        case STRING:
                                        case DATE:
                                        case TIME:
                                        case TIMESTAMP:
                                            return String.format(
                                                    "%s.%s between '%s' and '%s'",
                                                    conf.getRedshiftTable(),
                                                    sortColumn,
                                                    entry.getValue().left,
                                                    entry.getValue().right);
                                        case TINYINT:
                                        case SMALLINT:
                                        case INT:
                                        case BIGINT:
                                        case FLOAT:
                                        case DOUBLE:
                                        case DECIMAL:
                                            return String.format(
                                                    "%s.%s between %s and %s",
                                                    conf.getRedshiftTable(),
                                                    sortColumn,
                                                    entry.getValue().left,
                                                    entry.getValue().right);
                                        default:
                                            throw new S3RedshiftConnectorException(
                                                    S3RedshiftConnectorErrorCode
                                                            .UNSUPPORTED_MERGE_CONDITION_FIELD,
                                                    String.format(
                                                            "field name:%s, field type:%s",
                                                            sortColumn, fieldType.getSqlType()));
                                    }
                                })
                        .collect(Collectors.joining(" OR "));

        if (!StringUtils.isEmpty(sortKeyCondition)) {
            conditionClause = conditionClause + " AND (" + sortKeyCondition + ")";
        }

        String matchedClause =
                Stream.of(rowType.getFieldNames())
                        .filter(field -> !conf.getRedshiftTablePrimaryKeys().contains(field))
                        .map(field -> String.format("%s = source.%s", field, field))
                        .collect(Collectors.joining(", ", "UPDATE SET ", ""));
        String sinkFieldsClause = String.join(",", rowType.getFieldNames());
        String selectSourceFieldsClause =
                Stream.of(rowType.getFieldNames())
                        .map(field -> String.format("source.%s", field))
                        .collect(Collectors.joining(","));

        String sourceTable = conf.getSchema() + "." + conf.getTemporaryTableName();
        return String.format(
                "MERGE INTO %s.%s "
                        + "USING %s AS source "
                        + "ON %s "
                        + "WHEN MATCHED THEN %s "
                        + "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
                conf.getSchema(),
                conf.getRedshiftTable(),
                sourceTable,
                conditionClause,
                matchedClause,
                sinkFieldsClause,
                selectSourceFieldsClause);
    }

    private ImmutablePair<String[], String> generateTemporaryTableSortKeyValueQuerySql() {
        List<String> sortKey = getTemporaryTableSortKey();
        if (CollectionUtils.isEmpty(sortKey)) {
            throw new S3RedshiftConnectorException(
                    S3RedshiftConnectorErrorCode.MERGE_MUST_HAVE_PRIMARY_KEY,
                    ", table: " + conf.getRedshiftTable());
        }

        String conditionClause =
                sortKey.stream()
                        .map(field -> String.format("min(%s),max(%s)", field, field))
                        .collect(Collectors.joining(","));

        String sourceTable = conf.getSchema() + "." + conf.getTemporaryTableName();

        String[] sortKeys = sortKey.toArray(new String[0]);
        return new ImmutablePair<>(
                sortKeys, String.format("select %s from %s", conditionClause, sourceTable));
    }

    public String generateAnalyseSql(@NonNull String[] sortKeys) {
        String columns = String.join(", ", sortKeys);
        return String.format(
                "ANALYZE %s.%s(%s)", conf.getSchema(), conf.getRedshiftTable(), columns);
    }

    private List<String> getPrimaryKeys() {
        if (table != null) {
            return getPrimaryKeys(table);
        }
        if (conf.getRedshiftTablePrimaryKeys() != null) {
            return conf.getRedshiftTablePrimaryKeys();
        }
        return Collections.emptyList();
    }

    private List<String> getPrimaryKeys(CatalogTable table) {
        TableSchema tableSchema = table.getTableSchema();
        PrimaryKey primaryKey = tableSchema.getPrimaryKey();
        if (primaryKey != null && !primaryKey.getColumnNames().isEmpty()) {
            return primaryKey.getColumnNames();
        }
        return Collections.emptyList();
    }

    private List<String> getTemporaryTableSortKey() {
        if (temporaryTable != null) {
            List<String> primaryKeys = getPrimaryKeys(temporaryTable);
            if (CollectionUtils.isNotEmpty(primaryKeys)) {
                return primaryKeys;
            } else {
                log.warn("There are tables, but there is no primary key on the table");
                return conf.getRedshiftTablePrimaryKeys();
            }
        } else if (conf.getRedshiftTablePrimaryKeys() != null) {
            return conf.getRedshiftTablePrimaryKeys();
        }
        return Collections.emptyList();
    }

    private List<String> getTableSortKey() {
        if (table != null) {
            return getPrimaryKeys(table);
        } else if (conf.getRedshiftTablePrimaryKeys() != null) {
            return conf.getRedshiftTablePrimaryKeys();
        }
        return Collections.emptyList();
    }

    private String generateColumnDefinition(CatalogTable table) {
        TableSchema tableSchema = table.getTableSchema();
        String tableColumnDefinition =
                tableSchema.getColumns().stream()
                        .map(
                                column ->
                                        String.format(
                                                "%s %s",
                                                column.getName(),
                                                ToRedshiftTypeConverter.INSTANCE.convert(column)))
                        .collect(Collectors.joining(", "));

        PrimaryKey primaryKey = tableSchema.getPrimaryKey();
        if (primaryKey != null && !primaryKey.getColumnNames().isEmpty()) {
            String primaryKeyDefinition =
                    String.format(
                            "PRIMARY KEY (%s)",
                            String.join(",", tableSchema.getPrimaryKey().getColumnNames()));
            tableColumnDefinition = String.join(", ", tableColumnDefinition, primaryKeyDefinition);
        }
        return tableColumnDefinition;
    }

    private String generateColumnDefinition(SeaTunnelRowType rowType, List<String> primaryKeys) {
        String tableColumnDefinition =
                IntStream.range(0, rowType.getTotalFields())
                        .mapToObj(
                                i ->
                                        String.format(
                                                "%s %s",
                                                rowType.getFieldName(i),
                                                ToRedshiftTypeConverter.INSTANCE.convert(
                                                        rowType.getFieldType(i))))
                        .collect(Collectors.joining(", "));

        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            String primaryKeyDefinition =
                    String.format("PRIMARY KEY (%s)", String.join(",", primaryKeys));
            tableColumnDefinition = String.join(", ", tableColumnDefinition, primaryKeyDefinition);
        }
        return tableColumnDefinition;
    }

    private String getBucket(String bucket) {
        if (bucket.startsWith("s3a://")) {
            return bucket.replace("s3a://", "s3://");
        }
        if (bucket.startsWith("s3n://")) {
            return bucket.replace("s3n://", "s3://");
        }
        return bucket;
    }
}
