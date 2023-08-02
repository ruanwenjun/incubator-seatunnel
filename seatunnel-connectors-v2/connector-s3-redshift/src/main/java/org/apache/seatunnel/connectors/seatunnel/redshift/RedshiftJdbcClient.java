package org.apache.seatunnel.connectors.seatunnel.redshift;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConf;
import org.apache.seatunnel.connectors.seatunnel.redshift.exception.S3RedshiftJdbcConnectorException;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;

public class RedshiftJdbcClient implements AutoCloseable {
    private final HikariDataSource dataSource;

    public RedshiftJdbcClient(String jdbcUrl, String user, String password, int maxPoolSize) {
        this(jdbcUrl, user, password, maxPoolSize, Duration.ofMinutes(30));
    }

    public RedshiftJdbcClient(
            String jdbcUrl, String user, String password, int maxPoolSize, Duration maxIdleTime) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(user);
        config.setPassword(password);
        config.setDriverClassName("com.amazon.redshift.jdbc42.Driver");
        config.setMaximumPoolSize(maxPoolSize);
        config.setIdleTimeout(maxIdleTime.toMillis());
        this.dataSource = new HikariDataSource(config);
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public boolean execute(String sql) throws SQLException {
        try (Connection connection = getConnection()) {
            return connection.createStatement().execute(sql);
        }
    }

    @Override
    public void close() {
        dataSource.close();
    }

    public Integer executeQueryForNum(String sql) throws Exception {
        try (Connection connection = getConnection()) {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            if (resultSet == null) {
                return 0;
            }
            resultSet.next();
            return resultSet.getInt(1);
        } catch (SQLException e) {
            throw new S3RedshiftJdbcConnectorException(
                    CommonErrorCode.SQL_OPERATION_FAILED,
                    String.format("Execute sql failed, sql is %s ", sql),
                    e);
        }
    }

    public static RedshiftJdbcClient newSingleConnection(S3RedshiftConf conf) {
        return newConnectionPool(conf, 1);
    }

    public static RedshiftJdbcClient newConnectionPool(S3RedshiftConf conf, int maxPoolSize) {
        return new RedshiftJdbcClient(
                conf.getJdbcUrl(), conf.getJdbcUser(), conf.getJdbcPassword(), maxPoolSize);
    }

    public static RedshiftJdbcClient newConnectionPool(
            S3RedshiftConf conf, int maxPoolSize, Duration maxIdleTime) {
        return new RedshiftJdbcClient(
                conf.getJdbcUrl(),
                conf.getJdbcUser(),
                conf.getJdbcPassword(),
                maxPoolSize,
                maxIdleTime);
    }
}
