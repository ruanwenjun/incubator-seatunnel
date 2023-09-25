package org.apache.seatunnel.connectors.seatunnel.mongodb.catalog;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbCollectionProvider;

import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Collections;
import java.util.HashMap;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MongoDBCatalogTest {

    MongodbClientProvider clientProvider =
            MongodbCollectionProvider.builder()
                    .connectionString("mongodb://root:123456@127.0.0.1:27017")
                    .database("liulitest")
                    .collection("user")
                    .build();

    Catalog catalog = new MongoDBCatalog(clientProvider);

    String databaseName = "liulitest";
    String tableName = "catalog_test_user_info";
    TableIdentifier tableIdentifier = TableIdentifier.of("mongodb", databaseName, null, tableName);

    TablePath tablePath = tableIdentifier.toTablePath();

    @Order(1)
    @Test
    void open() {
        catalog.open();
    }

    @Order(2)
    @Test
    void getDefaultDatabase() {
        Assertions.assertEquals(databaseName, catalog.getDefaultDatabase());
    }

    @Order(3)
    @Test
    void databaseExists() {
        Assertions.assertTrue(catalog.databaseExists(databaseName));
    }

    @Order(4)
    @Test
    void listDatabases() {
        Assertions.assertTrue(catalog.listDatabases().contains(databaseName));
    }

    @Order(5)
    @Test
    void createTable() {
        CatalogTable catalogTable = buildTestTable(tableIdentifier);
        Assertions.assertFalse(catalog.tableExists(tableIdentifier.toTablePath()));
        catalog.createTable(tableIdentifier.toTablePath(), catalogTable, false);
        Assertions.assertTrue(catalog.tableExists(tableIdentifier.toTablePath()));
    }

    @Order(6)
    @Test
    void listTables() {
        Assertions.assertTrue(catalog.listTables(databaseName).contains(tableName));
    }

    @Order(7)
    @Test
    void tableExists() {
        Assertions.assertTrue(catalog.tableExists(tableIdentifier.toTablePath()));
    }

    @Order(8)
    @Test
    void isExistsData() {
        clientProvider
                .getClient()
                .getDatabase(databaseName)
                .getCollection(tableName)
                .insertOne(
                        new Document(
                                new HashMap<String, Object>() {
                                    {
                                        put("_id", String.valueOf(System.currentTimeMillis()));
                                        put("name", "liuli");
                                        put("age", 18);
                                    }
                                }));
        Assertions.assertTrue(catalog.isExistsData(tableIdentifier.toTablePath()));
    }

    @Order(9)
    @Test
    void truncateTable() {
        Assertions.assertTrue(catalog.isExistsData(tableIdentifier.toTablePath()));
        catalog.truncateTable(tableIdentifier.toTablePath(), false);
        Assertions.assertFalse(catalog.isExistsData(tableIdentifier.toTablePath()));
    }

    @Order(10)
    @Test
    void dropTable() {
        catalog.dropTable(tableIdentifier.toTablePath(), false);
        Assertions.assertFalse(catalog.tableExists(tableIdentifier.toTablePath()));
    }

    @Order(11)
    @Test
    void getTable() {
        Assertions.assertThrowsExactly(
                UnsupportedOperationException.class,
                () -> {
                    catalog.getTable(tableIdentifier.toTablePath());
                });
    }

    @Order(12)
    @Test
    void createDatabase() {
        Assertions.assertThrowsExactly(
                UnsupportedOperationException.class,
                () -> {
                    catalog.createDatabase(tableIdentifier.toTablePath(), false);
                });
    }

    @Order(13)
    @Test
    void dropDatabase() {
        Assertions.assertThrowsExactly(
                UnsupportedOperationException.class,
                () -> {
                    catalog.dropDatabase(tableIdentifier.toTablePath(), false);
                });
    }

    @Order(10000)
    @Test
    void close() {
        catalog.close();
    }

    CatalogTable buildTestTable(TableIdentifier tableIdentifier) {
        TableSchema.Builder builder = TableSchema.builder();
        builder.column(
                PhysicalColumn.of("_id", BasicType.STRING_TYPE, null, true, null, "test comment"));
        builder.column(
                PhysicalColumn.of("name", BasicType.STRING_TYPE, null, true, null, "test comment"));
        builder.column(
                PhysicalColumn.of("age", BasicType.INT_TYPE, null, true, null, "test comment"));
        TableSchema schema = builder.build();
        HashMap<String, String> options = new HashMap<>();

        return CatalogTable.of(
                tableIdentifier, schema, options, Collections.singletonList("name"), "null");
    }
}
