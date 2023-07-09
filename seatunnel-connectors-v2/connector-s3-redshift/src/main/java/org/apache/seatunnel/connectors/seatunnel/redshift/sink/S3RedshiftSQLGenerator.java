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
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConf;
import org.apache.seatunnel.connectors.seatunnel.redshift.datatype.ToRedshiftTypeConverter;

import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Getter
@ToString
public class S3RedshiftSQLGenerator implements Serializable {
    private final S3RedshiftConf conf;
    private final CatalogTable table;
    private final SeaTunnelRowType rowType;
    private final String createTableSQL;
    private String createTemporaryTableSQL;
    private String copyS3FileToTemporaryTableSql;
    private String cleanTemporaryTableSql;
    private String dropTemporaryTableSql;
    private String createExternalTableSql;
    private String dropExternalTableSql;
    private String mergeSql;

    public S3RedshiftSQLGenerator(S3RedshiftConf conf, CatalogTable table) {
        this(conf, table, table.getTableSchema().toPhysicalRowDataType());
    }

    public S3RedshiftSQLGenerator(S3RedshiftConf conf, SeaTunnelRowType rowType) {
        this(conf, null, rowType);
    }

    public S3RedshiftSQLGenerator(
            S3RedshiftConf conf, CatalogTable table, SeaTunnelRowType rowType) {
        this.conf = conf;
        this.table = table;
        this.rowType = rowType;
        this.createTableSQL = generateCreateTableSQL();
        if (conf.isCopyS3FileToTemporaryTableMode()) {
            this.createTemporaryTableSQL = generateCreateTemporaryTableSQL();
            this.copyS3FileToTemporaryTableSql = generateCopyS3FileToTemporaryTableSql();
            this.cleanTemporaryTableSql = generateCleanTemporaryTableSql();
            this.dropTemporaryTableSql = generateDropTemporaryTableSql();
        } else if (conf.isS3ExternalTableMode()) {
            this.createExternalTableSql = generateCreateExternalTableSql();
            this.dropExternalTableSql = generateDropExternalTableSql();
            this.mergeSql = generateMergeSql();
        }
    }

    public String generateCreateTableSQL() {
        return String.format(
                "CREATE TABLE IF NOT EXISTS %s (%s)",
                conf.getRedshiftTable(), generateColumnDefinition());
    }

    public String generateCreateTemporaryTableSQL() {
        return String.format(
                "CREATE TABLE IF NOT EXISTS %s (%s)",
                conf.getTemporaryTableName(), generateColumnDefinition());
    }

    public String generateCopyS3FileToTemporaryTableSql() {
        String bucket = getBucket(conf.getS3Bucket());
        if (bucket.endsWith("/")) {
            bucket = bucket.substring(0, bucket.lastIndexOf("/"));
        }
        String columns = String.join(",", rowType.getFieldNames());
        if (!Strings.isNullOrEmpty(conf.getAccessKey())
                && !Strings.isNullOrEmpty(conf.getSecretKey())) {
            return String.format(
                    "COPY %s(%s) FROM '%s/${path}' ACCESS_KEY_ID '%s' SECRET_ACCESS_KEY '%s' FORMAT ORC SERIALIZETOJSON",
                    conf.getTemporaryTableName(),
                    columns,
                    bucket,
                    conf.getAccessKey(),
                    conf.getSecretKey());
        }
        if (!Strings.isNullOrEmpty(conf.getRedshiftS3IamRole())) {
            return String.format(
                    "COPY %s(%s) FROM '%s/${path}' IAM_ROLE '%s' FORMAT ORC SERIALIZETOJSON",
                    conf.getTemporaryTableName(), columns, bucket, conf.getRedshiftS3IamRole());
        }
        throw new IllegalArgumentException("Either accessKey/secretKey or iamRole must be set");
    }

    public String generateCleanTemporaryTableSql() {
        return String.format("TRUNCATE TABLE %s", conf.getTemporaryTableName());
    }

    public String generateDropTemporaryTableSql() {
        return String.format("DROP TABLE IF EXISTS %s", conf.getTemporaryTableName());
    }

    public String generateCreateExternalTableSql() {
        String bucket = getBucket(conf.getS3Bucket());
        if (bucket.endsWith("/")) {
            bucket = bucket.substring(0, bucket.lastIndexOf("/"));
        }
        return String.format(
                "CREATE EXTERNAL TABLE %s.%s(%s) STORED AS orc LOCATION '%s/${dir}'",
                conf.getRedshiftExternalSchema(),
                conf.getTemporaryTableName(),
                generateColumnDefinition(),
                bucket);
    }

    public String generateDropExternalTableSql() {
        return String.format(
                "DROP TABLE IF EXISTS %s.%s",
                conf.getRedshiftExternalSchema(), conf.getTemporaryTableName());
    }

    public String generateMergeSql() {
        String conditionClause =
                conf.getRedshiftTablePrimaryKeys().stream()
                        .map(
                                field ->
                                        String.format(
                                                "%s.%s = source.%s",
                                                conf.getRedshiftTable(), field, field))
                        .collect(Collectors.joining(" AND "));
        String matchedClause = "DELETE";
        if (S3RedshiftChangelogMode.APPEND_ON_DUPLICATE_UPDATE.equals(conf.getChangelogMode())) {
            matchedClause =
                    Stream.of(rowType.getFieldNames())
                            .filter(field -> !conf.getRedshiftTablePrimaryKeys().contains(field))
                            .map(field -> String.format("%s = source.%s", field, field))
                            .collect(Collectors.joining(", ", "UPDATE SET ", ""));
        }
        String sinkFieldsClause = String.join(",", rowType.getFieldNames());
        String selectSourceFieldsClause =
                Stream.of(rowType.getFieldNames())
                        .map(field -> String.format("source.%s", field))
                        .collect(Collectors.joining(","));

        String sourceTable =
                conf.isCopyS3FileToTemporaryTableMode()
                        ? conf.getTemporaryTableName()
                        : conf.getRedshiftExternalSchema() + "." + conf.getTemporaryTableName();
        return String.format(
                "MERGE INTO %s "
                        + "USING %s AS source "
                        + "ON %s "
                        + "WHEN MATCHED THEN %s "
                        + "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
                conf.getRedshiftTable(),
                sourceTable,
                conditionClause,
                matchedClause,
                sinkFieldsClause,
                selectSourceFieldsClause);
    }

    private String generateColumnDefinition() {
        if (table != null) {
            TableSchema tableSchema = table.getTableSchema();
            return tableSchema.getColumns().stream()
                    .map(
                            column ->
                                    String.format(
                                            "%s %s",
                                            column.getName(),
                                            ToRedshiftTypeConverter.INSTANCE.convert(column)))
                    .collect(Collectors.joining(", "));
        }
        return IntStream.range(0, rowType.getTotalFields())
                .mapToObj(
                        i ->
                                String.format(
                                        "%s %s",
                                        rowType.getFieldName(i),
                                        ToRedshiftTypeConverter.INSTANCE.convert(
                                                rowType.getFieldType(i))))
                .collect(Collectors.joining(", "));
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
