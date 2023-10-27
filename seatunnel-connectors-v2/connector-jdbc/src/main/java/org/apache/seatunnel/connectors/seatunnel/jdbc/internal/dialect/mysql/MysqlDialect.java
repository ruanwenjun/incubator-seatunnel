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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MysqlDataTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.SQLUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import org.apache.commons.lang3.StringUtils;

import com.mysql.cj.MysqlType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class MysqlDialect implements JdbcDialect {
    public String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

    public MysqlDialect() {}

    public MysqlDialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.MYSQL;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new MysqlJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new MySqlTypeMapper();
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + getFieldIde(identifier, fieldIde) + "`";
    }

    @Override
    public String quoteDatabaseIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        String updateClause =
                Arrays.stream(fieldNames)
                        .map(
                                fieldName ->
                                        quoteIdentifier(fieldName)
                                                + "=VALUES("
                                                + quoteIdentifier(fieldName)
                                                + ")")
                        .collect(Collectors.joining(", "));
        String upsertSQL =
                getInsertIntoStatement(database, tableName, fieldNames)
                        + " ON DUPLICATE KEY UPDATE "
                        + updateClause;
        return Optional.of(upsertSQL);
    }

    @Override
    public PreparedStatement creatPreparedStatement(
            Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        PreparedStatement statement =
                connection.prepareStatement(
                        queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(Integer.MIN_VALUE);
        return statement;
    }

    @Override
    public String extractTableName(TablePath tablePath) {
        return tablePath.getTableName();
    }

    @Override
    public List<String> getSQLFromSchemaChangeEvent(TablePath tablePath, SchemaChangeEvent event) {
        List<String> sqlList = new ArrayList<>();
        if (event instanceof AlterTableColumnsEvent) {
            ((AlterTableColumnsEvent) event)
                    .getEvents()
                    .forEach(
                            column -> {
                                if (column instanceof AlterTableChangeColumnEvent) {
                                    String sql =
                                            String.format(
                                                    "alter table %s CHANGE %s %s",
                                                    tablePath.getFullName(),
                                                    ((AlterTableChangeColumnEvent) column)
                                                            .getOldColumn(),
                                                    this.buildColumnIdentifySql(
                                                            ((AlterTableAddColumnEvent) column)
                                                                    .getColumn(),
                                                            fieldIde));
                                    sqlList.add(sql);
                                } else if (column instanceof AlterTableModifyColumnEvent) {
                                    String sql =
                                            String.format(
                                                    "alter table %s MODIFY COLUMN %s",
                                                    tablePath.getFullName(),
                                                    this.buildColumnIdentifySql(
                                                            ((AlterTableAddColumnEvent) column)
                                                                    .getColumn(),
                                                            fieldIde));
                                    sqlList.add(sql);
                                } else if (column instanceof AlterTableAddColumnEvent) {
                                    String sql =
                                            String.format(
                                                    "alter table %s add column %s ",
                                                    tablePath.getFullName(),
                                                    this.buildColumnIdentifySql(
                                                            ((AlterTableAddColumnEvent) column)
                                                                    .getColumn(),
                                                            fieldIde));
                                    sqlList.add(sql);
                                } else if (column instanceof AlterTableDropColumnEvent) {
                                    String sql =
                                            String.format(
                                                    "alter table %s drop column %s",
                                                    tablePath.getFullName(),
                                                    ((AlterTableDropColumnEvent) column)
                                                            .getColumn());
                                    sqlList.add(sql);
                                } else {
                                    throw new UnsupportedOperationException(
                                            "Unsupported event: " + event);
                                }
                            });
        }
        return sqlList;
    }

    private String buildColumnIdentifySql(Column column, String fieldIde) {
        final MysqlDataTypeConvertor mysqlDataTypeConvertor = new MysqlDataTypeConvertor();
        final List<String> columnSqls = new ArrayList<>();
        columnSqls.add(CatalogUtils.quoteIdentifier(column.getName(), fieldIde, "`"));
        boolean isSupportDef = true;
        // Column name
        SqlType dataType = column.getDataType().getSqlType();
        boolean isBytes = StringUtils.equals(dataType.name(), SqlType.BYTES.name());
        Long columnLength = column.getLongColumnLength();
        if (isBytes) {
            Long bitLen = column.getBitLen() == null ? columnLength : column.getBitLen();
            if (bitLen >= 0 && bitLen <= 64) {
                columnSqls.add(MysqlType.BIT.getName());
                columnSqls.add("(" + (bitLen == 0 ? 1 : bitLen) + ")");
            } else {
                bitLen = bitLen == -1 ? bitLen : bitLen >> 3;
                if (bitLen >= 0 && bitLen <= 255) {
                    columnSqls.add(MysqlType.TINYBLOB.getName());
                } else if (bitLen <= 16383) {
                    columnSqls.add(MysqlType.BLOB.getName());
                } else if (bitLen <= 16777215) {
                    columnSqls.add(MysqlType.MEDIUMBLOB.getName());
                } else {
                    columnSqls.add(MysqlType.LONGBLOB.getName());
                }
                isSupportDef = false;
            }
        } else {
            if (columnLength >= 16383 && columnLength <= 65535) {
                columnSqls.add(MysqlType.TEXT.getName());
                isSupportDef = false;
            } else if (columnLength >= 65535 && columnLength <= 16777215) {
                columnSqls.add(MysqlType.MEDIUMTEXT.getName());
                isSupportDef = false;
            } else if (columnLength > 16777215) {
                columnSqls.add(MysqlType.LONGTEXT.getName());
                isSupportDef = false;
            } else {
                // Column type
                columnSqls.add(
                        mysqlDataTypeConvertor
                                .toConnectorType(column.getDataType(), null)
                                .getName());
                // Column length
                // add judge is need column legth
                if (column.getColumnLength() != null) {
                    final String name =
                            mysqlDataTypeConvertor
                                    .toConnectorType(column.getDataType(), null)
                                    .getName();
                    String fieSql = "";
                    List<String> list = new ArrayList<>();
                    list.add(MysqlType.VARCHAR.getName());
                    list.add(MysqlType.CHAR.getName());
                    list.add(MysqlType.BIGINT.getName());
                    list.add(MysqlType.INT.getName());
                    list.add(MysqlType.SMALLINT.getName());
                    if (StringUtils.equals(name, MysqlType.DECIMAL.getName())) {
                        DecimalType decimalType = (DecimalType) column.getDataType();
                        fieSql =
                                String.format(
                                        "(%d, %d)",
                                        decimalType.getPrecision(), decimalType.getScale());
                        columnSqls.add(fieSql);
                    } else if (list.contains(name)) {
                        if (MysqlType.VARCHAR.getName().equals(name)
                                && column.getColumnLength() <= 0) {
                            fieSql = "(" + "16367" + ")";
                        } else if (MysqlType.CHAR.getName().equals(name)
                                && column.getColumnLength() <= 0) {
                            fieSql = "(" + "255" + ")";
                        } else if (MysqlType.SMALLINT.getName().equals(name)
                                && column.getColumnLength() <= 0) {
                            fieSql = "(" + "6" + ")";
                        } else if (MysqlType.BIGINT.getName().equals(name)
                                && column.getColumnLength() <= 0) {
                            fieSql = "(" + "20" + ")";
                        } else if (MysqlType.INT.getName().equals(name)
                                && column.getColumnLength() <= 0) {
                            fieSql = "(" + "11" + ")";
                        } else {
                            fieSql = "(" + column.getColumnLength() + ")";
                        }
                        columnSqls.add(fieSql);
                    }
                }
            }
        }
        // nullable
        if (column.isNullable()) {
            columnSqls.add("NULL");
        } else {
            columnSqls.add("NOT NULL");
        }
        if (column.getComment() != null) {
            columnSqls.add("COMMENT '" + column.getComment() + "'");
        }
        return String.join(" ", columnSqls);
    }

    @Override
    public TablePath parse(String tablePath) {
        return TablePath.of(tablePath, false);
    }

    @Override
    public Long approximateRowCntStatement(Connection connection, JdbcSourceTable table)
            throws SQLException {
        if (StringUtils.isBlank(table.getQuery())) {
            // The statement used to get approximate row count which is less
            // accurate than COUNT(*), but is more efficient for large table.
            TablePath tablePath = table.getTablePath();
            String useDatabaseStatement =
                    String.format("USE %s;", quoteDatabaseIdentifier(tablePath.getDatabaseName()));
            String rowCountQuery =
                    String.format("SHOW TABLE STATUS LIKE '%s';", tablePath.getTableName());
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(useDatabaseStatement);
                try (ResultSet rs = stmt.executeQuery(rowCountQuery)) {
                    if (!rs.next() || rs.getMetaData().getColumnCount() < 5) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]",
                                        rowCountQuery));
                    }
                    return rs.getLong(5);
                }
            }
        }

        return SQLUtils.countForSubquery(connection, table.getQuery());
    }
}
