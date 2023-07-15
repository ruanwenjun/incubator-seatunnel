package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter
public class ConnectionPoolManager {

    private final HikariDataSource connectionPool;

    private final Map<Integer, Connection> connectionMap;

    ConnectionPoolManager(HikariDataSource connectionPool) {
        this.connectionPool = connectionPool;
        connectionMap = new ConcurrentHashMap<>();
    }

    public Connection getConnection(int index) {
        return connectionMap.computeIfAbsent(
                index,
                i -> {
                    try {
                        return connectionPool.getConnection();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    public boolean containsConnection(int index) {
        return connectionMap.containsKey(index);
    }

    public Connection remove(int index) {
        return connectionMap.remove(index);
    }

    public String getPoolName() {
        return connectionPool.getPoolName();
    }

    public void close() {
        if (!connectionPool.isClosed()) {
            connectionPool.close();
        }
    }
}
