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
import org.apache.seatunnel.connectors.seatunnel.redshift.exception.S3RedshiftJdbcConnectorException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class RedshiftJdbcClient {

    private static volatile RedshiftJdbcClient INSTANCE = null;

    private final Connection connection;

    public static RedshiftJdbcClient getInstance(S3RedshiftConf config)
            throws S3RedshiftJdbcConnectorException {
        if (INSTANCE == null) {
            synchronized (RedshiftJdbcClient.class) {
                if (INSTANCE == null) {

                    try {
                        INSTANCE =
                                new RedshiftJdbcClient(
                                        config.getJdbcUrl(),
                                        config.getJdbcUser(),
                                        config.getJdbcPassword());
                    } catch (SQLException | ClassNotFoundException e) {
                        throw new S3RedshiftJdbcConnectorException(
                                CommonErrorCode.SQL_OPERATION_FAILED,
                                "RedshiftJdbcClient init error",
                                e);
                    }
                }
            }
        }
        return INSTANCE;
    }

    private RedshiftJdbcClient(String url, String user, String password)
            throws SQLException, ClassNotFoundException {
        Class.forName("com.amazon.redshift.jdbc42.Driver");
        this.connection = DriverManager.getConnection(url, user, password);
    }

    public boolean checkTableExists(String tableName) {
        boolean flag = false;
        try {
            DatabaseMetaData meta = connection.getMetaData();
            String[] type = {"TABLE"};
            ResultSet rs = meta.getTables(null, null, tableName, type);
            flag = rs.next();
        } catch (SQLException e) {
            throw new S3RedshiftJdbcConnectorException(
                    CommonErrorCode.TABLE_SCHEMA_GET_FAILED,
                    String.format(
                            "Check table is or not existed failed, table name is %s ", tableName),
                    e);
        }
        return flag;
    }

    public boolean execute(String sql) throws Exception {
        try (Statement statement = connection.createStatement()) {
            return statement.execute(sql);
        } catch (SQLException e) {
            throw new S3RedshiftJdbcConnectorException(
                    CommonErrorCode.SQL_OPERATION_FAILED,
                    String.format("Execute sql failed, sql is %s ", sql),
                    e);
        }
    }

    public Integer executeQueryForNum(String sql) throws Exception {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
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

    public boolean existDataForSql(String sql) throws Exception{
        return executeQueryForNum(sql) > 0;
    }


    public void close() throws SQLException {
        synchronized (RedshiftJdbcClient.class) {
            connection.close();
            INSTANCE = null;
        }
    }
}
