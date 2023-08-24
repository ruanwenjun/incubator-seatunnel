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

package org.apache.seatunnel.connectors.seatunnel.redshift;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConf;
import org.apache.seatunnel.connectors.seatunnel.redshift.exception.S3RedshiftConnectorException;

import org.apache.commons.lang3.tuple.ImmutablePair;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class RedshiftJdbcClient implements AutoCloseable {
    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final int maxPoolSize;
    private final Duration maxIdleTime;

    private volatile HikariDataSource dataSource;

    public RedshiftJdbcClient(String jdbcUrl, String user, String password, int maxPoolSize) {
        this(jdbcUrl, user, password, maxPoolSize, Duration.ofMinutes(30));
    }

    public Connection getConnection() throws SQLException {
        if (dataSource == null) {
            synchronized (this) {
                if (dataSource == null) {
                    HikariConfig config = new HikariConfig();
                    config.setJdbcUrl(jdbcUrl);
                    config.setUsername(user);
                    config.setPassword(password);
                    config.setDriverClassName("com.amazon.redshift.jdbc42.Driver");
                    config.setMaximumPoolSize(maxPoolSize);
                    config.setIdleTimeout(maxIdleTime.toMillis());
                    dataSource = new HikariDataSource(config);
                }
            }
        }
        return dataSource.getConnection();
    }

    public boolean execute(String sql) throws SQLException {
        try (Connection connection = getConnection()) {
            return connection.createStatement().execute(sql);
        } catch (SQLException e) {
            log.error("Execute sql failed, sql is {}", sql, e);
            throw e;
        }
    }

    public int executeUpdate(String sql) throws SQLException {
        try (Connection connection = getConnection()) {
            return connection.createStatement().executeUpdate(sql);
        } catch (SQLException e) {
            log.error("Execute sql failed, sql is {}", sql, e);
            throw e;
        }
    }

    @Override
    public void close() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    public boolean existDataForSql(String sql) throws SQLException {
        try (Connection connection = getConnection()) {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            return resultSet.next();
        }
    }

    public Integer executeQueryCount(String sql) throws SQLException {
        try (Connection connection = getConnection()) {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            if (!resultSet.next()) {
                return 0;
            }
            return resultSet.getInt(1);
        }
    }

    public Map<String, ImmutablePair<Object, Object>> querySortValues(String sql, String[] sortKeys)
            throws Exception {
        Map<String, ImmutablePair<Object, Object>> result = new HashMap<>();
        try (Connection connection = getConnection();
                Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                for (int i = 1; i < sortKeys.length + 1; i++) {
                    int j = i * 2;

                    String key = sortKeys[i - 1];
                    Object min = resultSet.getObject(j - 1);
                    Object max = resultSet.getObject(j);
                    if (min == null || max == null) {
                        continue;
                    }
                    result.put(key, new ImmutablePair<>(min, max));
                }
            }
            return result;
        } catch (SQLException e) {
            throw new S3RedshiftConnectorException(
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
