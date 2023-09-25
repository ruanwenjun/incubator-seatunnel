package org.apache.seatunnel.connectors.seatunnel.mongodb.catalog;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbClientProvider;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.List;

public class MongoDBCatalog implements Catalog {

    MongodbClientProvider clientProvider;

    public MongoDBCatalog(MongodbClientProvider clientProvider) {
        this.clientProvider = clientProvider;
    }

    @Override
    public void open() {
        clientProvider.getClient();
    }

    @Override
    public void close() {
        clientProvider.close();
    }

    @Override
    public String getDefaultDatabase() {
        return clientProvider.getDefaultDatabase().getName();
    }

    @Override
    public boolean databaseExists(String databaseName) {
        for (String name : clientProvider.getClient().listDatabaseNames()) {
            if (name.equals(databaseName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<String> listDatabases() {
        return clientProvider.getClient().listDatabaseNames().into(new ArrayList<>());
    }

    @Override
    public List<String> listTables(String databaseName) {
        MongoDatabase database = clientProvider.getClient().getDatabase(databaseName);
        return database.listCollectionNames().into(new ArrayList<>());
    }

    @Override
    public boolean tableExists(TablePath tablePath) {
        MongoDatabase database =
                clientProvider.getClient().getDatabase(tablePath.getDatabaseName());
        for (String collectionName : database.listCollectionNames()) {
            if (collectionName.equals(tablePath.getTableName())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public CatalogTable getTable(TablePath tablePath) {
        // Convert MongoDB collection to CatalogTable object.
        // You'll need to implement this conversion depending on what you're storing in MongoDB.
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists) {
        MongoDatabase database =
                clientProvider.getClient().getDatabase(tablePath.getDatabaseName());
        database.createCollection(tablePath.getTableName());
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists) {
        MongoDatabase database =
                clientProvider.getClient().getDatabase(tablePath.getDatabaseName());
        database.getCollection(tablePath.getTableName()).drop();
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (tableExists(tablePath)) {
            MongoDatabase db = clientProvider.getClient().getDatabase(tablePath.getDatabaseName());
            MongoCollection<Document> collection = db.getCollection(tablePath.getTableName());
            collection.deleteMany(new Document());
        } else if (!ignoreIfNotExists) {
            throw new TableNotExistException("Table does not exist", tablePath);
        }
    }

    @Override
    public boolean isExistsData(TablePath tablePath) {
        if (tableExists(tablePath)) {
            MongoDatabase db = clientProvider.getClient().getDatabase(tablePath.getDatabaseName());
            MongoCollection<Document> collection = db.getCollection(tablePath.getTableName());
            return collection.countDocuments() > 0;
        } else {
            throw new TableNotExistException("Table does not exist", tablePath);
        }
    }
}
