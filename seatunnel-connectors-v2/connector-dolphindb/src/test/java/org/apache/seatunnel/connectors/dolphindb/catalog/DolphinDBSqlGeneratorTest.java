package org.apache.seatunnel.connectors.dolphindb.catalog;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

class DolphinDBSqlGeneratorTest {

    @Test
    void generateDeleteRowSql() {
        String[] fields = Lists.newArrayList("id", "name", "age").toArray(new String[0]);
        BasicType[] seaTunnelRowTypes =
                Lists.newArrayList(BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE)
                        .toArray(new BasicType[0]);
        SeaTunnelRowType seaTunnelRowType = new SeaTunnelRowType(fields, seaTunnelRowTypes);
        String sql =
                DolphinDBSqlGenerator.generateDeleteRowSql(
                        "dfs://whalescheduler", "users", seaTunnelRowType);
        Assertions.assertEquals(
                "delete from 'dfs://whalescheduler'.'users' where id = ? , name = ? , age = ?",
                sql);
    }
}
