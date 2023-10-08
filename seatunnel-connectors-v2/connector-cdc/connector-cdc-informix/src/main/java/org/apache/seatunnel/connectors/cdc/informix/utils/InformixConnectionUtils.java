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

package org.apache.seatunnel.connectors.cdc.informix.utils;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils;

import com.informix.jdbc.IfxPreparedStatement;
import com.informix.jdbc.IfxSqliConnect;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class InformixConnectionUtils {

    public static Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, String columnName)
            throws SQLException {
        String minMaxQuery =
                String.format(
                        "SELECT MIN(%s), MAX(%s) FROM %s",
                        quote(columnName), quote(columnName), quote(tableId));
        JdbcConnection.ResultSetMapper<Object[]> mapper =
                rs -> {
                    rs.next();
                    return SourceRecordUtils.rowToArray(rs, 2);
                };
        return jdbc.queryAndMap(minMaxQuery, mapper);
    }

    public static Object queryMin(
            JdbcConnection jdbc, TableId tableId, String columnName, Object excludedLowerBound)
            throws SQLException {
        String minQuery =
                String.format(
                        "SELECT MIN(%s) FROM %s WHERE %s > ?",
                        quote(columnName), quote(tableId), quote(columnName));
        JdbcConnection.ResultSetMapper<Object> mapper =
                rs -> {
                    rs.next();
                    return rs.getObject(1);
                };
        return jdbc.prepareQueryAndMap(minQuery, ps -> ps.setObject(1, excludedLowerBound), mapper);
    }

    public static Object[] sampleDataFromColumn(
            JdbcConnection jdbc, TableId tableId, String columnName, int inverseSamplingRate)
            throws SQLException {
        final String minQuery =
                String.format(
                        "SELECT %s FROM %s WHERE MOD((%s - (SELECT MIN(%s) FROM %s)), %s) = 0 ORDER BY %s",
                        quote(columnName),
                        quote(tableId),
                        quote(columnName),
                        quote(columnName),
                        quote(tableId),
                        inverseSamplingRate,
                        quote(columnName));
        return jdbc.queryAndMap(
                minQuery,
                resultSet -> {
                    List<Object> results = new ArrayList<>();
                    while (resultSet.next()) {
                        results.add(resultSet.getObject(1));
                    }
                    return results.toArray();
                });
    }

    public static Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            String splitColumnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        String quotedColumn = quote(splitColumnName);
        String query =
                String.format(
                        "SELECT MAX(%s) FROM ("
                                + "SELECT FIRST %s %s FROM %s WHERE %s >= ? ORDER BY %s ASC"
                                + ") AS T",
                        quotedColumn,
                        chunkSize,
                        quotedColumn,
                        quote(tableId),
                        quotedColumn,
                        quotedColumn);
        JdbcConnection.ResultSetMapper<Object> mapper =
                rs -> {
                    rs.next();
                    return rs.getObject(1);
                };

        return jdbc.prepareQueryAndMap(query, ps -> ps.setObject(1, includedLowerBound), mapper);
    }

    public static Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        String rowCountQuery = String.format("SELECT COUNT(*) FROM %s", quote(tableId));
        return jdbc.queryAndMap(
                rowCountQuery,
                rs -> {
                    rs.next();
                    return rs.getLong(1);
                });
    }

    public static String buildSplitQuery(
            TableId tableId, SeaTunnelRowType rowType, boolean isFirstSplit, boolean isLastSplit) {
        final String condition;
        if (isFirstSplit && isLastSplit) {
            condition = null;
        } else if (isFirstSplit) {
            String filterCondition =
                    Arrays.stream(rowType.getFieldNames())
                            .map(field -> field + "  <= ? ")
                            .collect(Collectors.joining(" AND "));
            String notCondition =
                    Arrays.stream(rowType.getFieldNames())
                            .map(field -> field + "  = ? ")
                            .collect(Collectors.joining(" AND "));
            condition = String.format("%s AND NOT (%s)", filterCondition, notCondition);
        } else if (isLastSplit) {
            condition =
                    Arrays.stream(rowType.getFieldNames())
                            .map(field -> field + "  >= ? ")
                            .collect(Collectors.joining(" AND "));
        } else {
            String filterCondition =
                    Stream.concat(
                                    Arrays.stream(rowType.getFieldNames())
                                            .map(field -> field + "  >= ? "),
                                    Arrays.stream(rowType.getFieldNames())
                                            .map(field -> field + "  <= ? "))
                            .collect(Collectors.joining(" AND "));
            String notCondition =
                    Arrays.stream(rowType.getFieldNames())
                            .map(field -> field + "  = ? ")
                            .collect(Collectors.joining(" AND "));
            condition = String.format("%s AND NOT (%s)", filterCondition, notCondition);
        }

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ").append(quote(tableId));
        if (condition != null) {
            sql.append(" WHERE ").append(condition);
        }
        return sql.toString();
    }

    public static PreparedStatement createTableSplitDataStatement(
            JdbcConnection jdbc,
            String sql,
            boolean isFirstSplit,
            boolean isLastSplit,
            Object[] splitStart,
            Object[] splitEnd,
            int primaryKeyNum,
            int fetchSize) {
        try {
            IfxSqliConnect connection = (IfxSqliConnect) jdbc.connection();
            IfxPreparedStatement statement =
                    (IfxPreparedStatement)
                            connection.prepareStatement(
                                    sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            statement.setFetchSize(fetchSize);
            statement.setFetchBufferSize(100000);
            if (isFirstSplit && isLastSplit) {
                return statement;
            }
            if (isFirstSplit) {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitEnd[i]);
                    statement.setObject(i + 1 + primaryKeyNum, splitEnd[i]);
                }
            } else if (isLastSplit) {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitStart[i]);
                }
            } else {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitStart[i]);
                    statement.setObject(i + 1 + primaryKeyNum, splitEnd[i]);
                    statement.setObject(i + 1 + 2 * primaryKeyNum, splitEnd[i]);
                }
            }
            return statement;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to build the split data read statement.", e);
        }
    }

    @SuppressWarnings("MagicNumber")
    public static List<Column> queryColumns(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        List<Column> columns = new ArrayList<>();
        try (ResultSet rs =
                jdbc.connection()
                        .getMetaData()
                        .getColumns(tableId.catalog(), tableId.schema(), tableId.table(), null)) {
            int i = 0;
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String fullTypeName = rs.getString("TYPE_NAME");
                int columnLength = rs.getInt("COLUMN_SIZE");
                int columnScale = rs.getInt("DECIMAL_DIGITS");
                boolean isNullable = rs.getInt("NULLABLE") == 1;
                int jdbcType = rs.getInt("SQL_DATA_TYPE");
                Column column =
                        Column.editor()
                                .name(columnName)
                                .type(fullTypeName)
                                .length(columnLength)
                                .scale(columnScale)
                                .optional(isNullable)
                                .position(i++)
                                .jdbcType(jdbcType)
                                .create();
                columns.add(column);
            }
        }
        String selectMetadataSQL =
                columns.stream()
                        .map(Column::name)
                        .collect(
                                Collectors.joining(
                                        ",", "SELECT FIRST 1 ", " FROM " + quote(tableId)));
        jdbc.query(
                selectMetadataSQL,
                rs -> {
                    ResultSetMetaData metadata = rs.getMetaData();
                    for (int i = 0; i < columns.size(); i++) {
                        Column newColumn =
                                columns.get(i)
                                        .edit()
                                        .jdbcType(metadata.getColumnType(i + 1))
                                        .create();
                        columns.set(i, newColumn);
                    }
                });
        return columns;
    }

    @SuppressWarnings("MagicNumber")
    public static List<String> queryPrimaryKeyNames(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        List<String> primaryKey = new ArrayList<>();
        try (ResultSet rs =
                jdbc.connection()
                        .getMetaData()
                        .getPrimaryKeys(tableId.catalog(), tableId.schema(), tableId.table())) {
            while (rs.next()) {
                primaryKey.add(rs.getString("COLUMN_NAME"));
            }
        }
        return primaryKey;
    }

    public static String quote(TableId tableId) {
        return tableId.catalog() + ":" + tableId.schema() + "." + tableId.table();
    }

    public static String quote(String dbOrTableName) {
        return dbOrTableName;
    }
}
