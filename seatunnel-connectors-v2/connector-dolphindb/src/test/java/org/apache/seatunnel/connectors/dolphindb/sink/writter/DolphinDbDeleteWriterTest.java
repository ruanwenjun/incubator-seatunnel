package org.apache.seatunnel.connectors.dolphindb.sink.writter;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.dolphindb.jdbc.JDBCConnection;
import com.xxdb.comm.SqlStdEnum;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

class DolphinDbDeleteWriterTest {

    @Disabled
    @Test
    void write() throws SQLException {
        Properties prop = new Properties();
        prop.setProperty("user", "admin");
        prop.setProperty("password", "123456");
        prop.setProperty("hostName", "localhost");
        prop.setProperty("port", "8848");
        prop.setProperty("databasePath", "dfs://whalescheduler");
        prop.setProperty("sqlStd", SqlStdEnum.DolphinDB.getName());
        String url = "jdbc:dolphindb://localhost:8848";
        try (JDBCConnection jdbcConnection = new JDBCConnection(url, prop);
                PreparedStatement preparedStatement =
                        jdbcConnection.prepareStatement(
                                "delete from st01_ws_users where userName = ? and id = ?"); ) {
            preparedStatement.setObject(1, "aa");
            preparedStatement.setObject(2, 2);
            boolean execute = preparedStatement.execute();
            System.out.println(execute);
        }
    }
}
