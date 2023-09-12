package org.apache.seatunnel.connectors.dolphindb.catalog;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import java.util.List;

@Disabled
class DolphinDBCatalogTest {

    public DolphinDBCatalog dolphinDBCatalog;

    @BeforeEach
    public void before() {
        dolphinDBCatalog =
                new DolphinDBCatalog(
                        "dolphindb",
                        Lists.newArrayList("localhost:8848"),
                        "admin",
                        "123456",
                        "dfs://whalescheduler",
                        "user",
                        false);
        dolphinDBCatalog.open();
    }

    @AfterEach
    public void after() {
        dolphinDBCatalog.close();
    }

    @Test
    void getDefaultDatabase() {
        String defaultDatabase = dolphinDBCatalog.getDefaultDatabase();
        Assertions.assertEquals("dfs://whalescheduler", defaultDatabase);
    }

    @Test
    void databaseExists() {
        Assertions.assertTrue(dolphinDBCatalog.databaseExists("dfs://whalescheduler"));
    }

    @Test
    void listDatabases() {
        List<String> listDatabases = dolphinDBCatalog.listDatabases();
        Assertions.assertTrue(listDatabases.contains("dfs://whalescheduler"));
    }

    @Test
    void listTables() {
        List<String> listTables = dolphinDBCatalog.listTables("dfs://whalescheduler");
        Assertions.assertTrue(listTables.contains("user"));
    }

    @Test
    void tableExists() {
        TablePath tablePath = new TablePath("dfs://whalescheduler", null, "user");
        Assertions.assertTrue(dolphinDBCatalog.tableExists(tablePath));
    }

    @Test
    void getTable() {
        TablePath tablePath = new TablePath("dfs://whalescheduler", null, "user");
        CatalogTable dbCatalogTable = dolphinDBCatalog.getTable(tablePath);
        System.out.println(dbCatalogTable);
    }

    @Test
    void createTable() {}

    @Test
    void dropTable() {}

    @Test
    void createDatabase() {}

    @Test
    void dropDatabase() {}

    @Test
    void dropTableInternal() {}

    @Test
    void truncateTable() {
        dolphinDBCatalog.truncateTable(new TablePath("dfs://whalescheduler", null, "user"), false);
    }
}
