package org.apache.seatunnel.connectors.dolphindb.sink.writter;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import com.dolphindb.jdbc.JDBCConnection;
import com.xxdb.comm.SqlStdEnum;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.ADDRESS;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.DATABASE;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.PASSWORD;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.TABLE;
import static org.apache.seatunnel.connectors.dolphindb.config.DolphinDBConfig.USER;

public class DolphinDbDeleteWriter implements DolphinDBWriter {

    private final ReadonlyConfig pluginConfig;
    private final SeaTunnelRowType seaTunnelRowType;
    private final JDBCConnection dbConnection;
    private final String deleteSql;

    public DolphinDbDeleteWriter(ReadonlyConfig pluginConfig, SeaTunnelRowType seaTunnelRowType)
            throws SQLException {
        this.pluginConfig = pluginConfig;
        this.seaTunnelRowType = seaTunnelRowType;
        this.dbConnection = createDbConnection();
        this.deleteSql = createDeleteSql();
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) {
        try (PreparedStatement preparedStatement = dbConnection.prepareStatement(deleteSql)) {
            Object[] fields = seaTunnelRow.getFields();
            for (int i = 0; i < fields.length; i++) {
                preparedStatement.setObject(i + 1, fields[i]);
            }
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Void> prepareCommit() throws Exception {
        return Optional.empty();
    }

    @Override
    public void close() throws Exception {
        try (JDBCConnection dbConnection1 = dbConnection) {}
    }

    private JDBCConnection createDbConnection() throws SQLException {
        List<String> addresses = pluginConfig.get(ADDRESS);
        Properties prop = new Properties();
        prop.setProperty("user", pluginConfig.get(USER));
        prop.setProperty("password", pluginConfig.get(PASSWORD));
        prop.setProperty("sqlStd", SqlStdEnum.DolphinDB.getName());
        String address = addresses.get(0);
        prop.setProperty("hostName", address.substring(0, address.lastIndexOf(":")));
        prop.setProperty("port", address.substring(address.lastIndexOf(":") + 1));

        String url = "jdbc:dolphindb://" + address;
        return new JDBCConnection(url, prop);
    }

    private String createDeleteSql() {
        String deleteSql =
                "delete from "
                        + pluginConfig.get(DATABASE)
                        + "."
                        + pluginConfig.get(TABLE)
                        + " where ";
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            deleteSql += fieldNames[i] + " = ?";
            if (i != fieldNames.length - 1) {
                deleteSql += " and ";
            }
        }
        return deleteSql;
    }
}
