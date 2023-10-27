package org.apache.seatunnel.connectors.selectdb.sink;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.event.SchemaChangeEvent;
import org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig;

import org.apache.commons.collections4.CollectionUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SelectDbDdlUtil {

    public static void executeDdl(SelectDBConfig selectDBConfig, SchemaChangeEvent event) {
        final List<String> ddlSqlList = getDdlSqlList(selectDBConfig, event);
        if (!CollectionUtils.isEmpty(ddlSqlList)) {
            executeDdlSql(ddlSqlList, selectDBConfig);
        }
    }

    private static List<String> getDdlSqlList(
            SelectDBConfig selectDBConfig, SchemaChangeEvent event) {
        TablePath tablePath = TablePath.of(selectDBConfig.getDatabase(), selectDBConfig.getTable());
        return getSQLFromSchemaChangeEvent(tablePath, event);
    }

    private static void executeDdlSql(List<String> ddlSqlList, SelectDBConfig selectDBConfig) {
        try (Connection conn =
                DriverManager.getConnection(
                        selectDBConfig.getJdbcUrl(),
                        selectDBConfig.getUsername(),
                        selectDBConfig.getPassword())) {
            final Statement statement = conn.createStatement();
            for (String ddlSql : ddlSqlList) {
                statement.execute(ddlSql);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static List<String> getSQLFromSchemaChangeEvent(
            TablePath tablePath, SchemaChangeEvent event) {
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
                                                    SelectDBSaveModeUtil.columnToStarrocksType(
                                                            ((AlterTableAddColumnEvent) column)
                                                                    .getColumn()));
                                    sqlList.add(sql);
                                } else if (column instanceof AlterTableModifyColumnEvent) {
                                    String sql =
                                            String.format(
                                                    "alter table %s MODIFY COLUMN %s",
                                                    tablePath.getFullName(),
                                                    SelectDBSaveModeUtil.columnToStarrocksType(
                                                            ((AlterTableAddColumnEvent) column)
                                                                    .getColumn()));
                                    sqlList.add(sql);
                                } else if (column instanceof AlterTableAddColumnEvent) {
                                    String sql =
                                            String.format(
                                                    "alter table %s add column %s ",
                                                    tablePath.getFullName(),
                                                    SelectDBSaveModeUtil.columnToStarrocksType(
                                                            ((AlterTableAddColumnEvent) column)
                                                                    .getColumn()));
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
}
