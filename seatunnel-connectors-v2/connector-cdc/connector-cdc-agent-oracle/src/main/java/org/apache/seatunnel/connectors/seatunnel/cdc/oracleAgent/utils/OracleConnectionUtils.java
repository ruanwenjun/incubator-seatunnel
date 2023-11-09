package org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.utils;

import org.apache.seatunnel.common.utils.SeaTunnelException;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.Scn;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;

@Slf4j
public class OracleConnectionUtils {

    /** show current scn sql in oracle. */
    private static final String SHOW_CURRENT_SCN = "SELECT CURRENT_SCN FROM V$DATABASE";

    /** Creates a new {@link OracleConnection}, but not open the connection. */
    public static OracleConnection createOracleConnection(Configuration dbzConfiguration) {
        Configuration configuration = dbzConfiguration.subset(DATABASE_CONFIG_PREFIX, true);

        return new OracleConnection(
                configuration.isEmpty() ? dbzConfiguration : configuration,
                OracleConnectionUtils.class::getClassLoader);
    }

    /** Fetch current redoLog offsets in Oracle Server. */
    public static Long currentRedoLogScn(JdbcConnection jdbc) {
        try {
            return jdbc.queryAndMap(
                    SHOW_CURRENT_SCN,
                    rs -> {
                        if (rs.next()) {
                            final String scn = rs.getString(1);
                            return Scn.valueOf(scn).longValue();
                        } else {
                            throw new SeaTunnelException(
                                    "Cannot read the scn via '"
                                            + SHOW_CURRENT_SCN
                                            + "'. Make sure your server is correctly configured");
                        }
                    });
        } catch (SQLException e) {
            throw new SeaTunnelException(
                    "Cannot read the redo log position via '"
                            + SHOW_CURRENT_SCN
                            + "'. Make sure your server is correctly configured",
                    e);
        }
    }

    public static String getTableOwner(JdbcConnection jdbcConnection, String table) {
        String queryTableOwnerSql =
                "SELECT OWNER FROM ALL_TABLES WHERE TABLE_NAME = '" + table.toUpperCase() + "'";
        try {
            return jdbcConnection.queryAndMap(
                    queryTableOwnerSql,
                    rs -> {
                        if (rs.next()) {
                            return rs.getString(1);
                        }
                        throw new RuntimeException(
                                "Query table owner failed, table:"
                                        + table
                                        + " due to the owner is null");
                    });
        } catch (Exception e) {
            throw new SeaTunnelException("Query table owner failed", e);
        }
    }

    public static List<TableId> listTables(
            JdbcConnection jdbcConnection, String database, RelationalTableFilters tableFilters)
            throws SQLException {
        final List<TableId> capturedTableIds = new ArrayList<>();

        Set<TableId> tableIdSet = new HashSet<>();
        String queryTablesSql =
                "SELECT OWNER ,TABLE_NAME,TABLESPACE_NAME FROM ALL_TABLES \n"
                        + "WHERE TABLESPACE_NAME IS NOT NULL AND TABLESPACE_NAME NOT IN ('SYSTEM','SYSAUX')";
        try {
            jdbcConnection.query(
                    queryTablesSql,
                    rs -> {
                        while (rs.next()) {
                            String schemaName = rs.getString(1);
                            String tableName = rs.getString(2);
                            TableId tableId = new TableId(database, schemaName, tableName);
                            tableIdSet.add(tableId);
                        }
                    });
        } catch (SQLException e) {
            log.warn(" SQL execute error, sql:{}", queryTablesSql, e);
        }

        for (TableId tableId : tableIdSet) {
            if (tableFilters.dataCollectionFilter().isIncluded(tableId)) {
                capturedTableIds.add(tableId);
                log.info("\t including '{}' for further processing", tableId);
            } else {
                log.debug("\t '{}' is filtered out of capturing", tableId);
            }
        }

        return capturedTableIds;
    }
}
