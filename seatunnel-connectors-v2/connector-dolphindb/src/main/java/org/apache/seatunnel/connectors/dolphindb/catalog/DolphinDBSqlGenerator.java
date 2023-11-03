package org.apache.seatunnel.connectors.dolphindb.catalog;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

public class DolphinDBSqlGenerator {

    public static String generateDeleteRowSql(
            String database, String table, SeaTunnelRowType seaTunnelRowType) {
        String deleteSql = "delete from " + table + " where ";
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            deleteSql += fieldNames[i] + " = ?";
            if (i != fieldNames.length - 1) {
                deleteSql += " , ";
            }
        }
        return deleteSql;
    }
}
