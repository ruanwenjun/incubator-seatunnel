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

package io.debezium.connector.dameng.logminer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.dameng.DamengConnection;
import io.debezium.connector.dameng.DamengStreamingChangeEventSourceMetrics;
import io.debezium.connector.dameng.Scn;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.util.Strings;

import java.math.BigInteger;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** This class contains methods to configure and manage LogMiner utility */
public class LogMinerHelper {

    private static final String CURRENT = "CURRENT";
    private static final String UNKNOWN = "unknown";
    private static final String TOTAL = "TOTAL";
    private static final String ALL_COLUMN_LOGGING = "ALL COLUMN LOGGING";
    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerHelper.class);

    public enum DATATYPE {
        LONG,
        TIMESTAMP,
        STRING,
        FLOAT
    }

    private static Map<String, DamengConnection> racFlushConnections = new HashMap<>();

    static void setNlsSessionParameters(JdbcConnection connection) throws SQLException {
        connection.executeWithoutCommitting(
                "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'");
        connection.executeWithoutCommitting(
                "ALTER SESSION SET NLS_TIMESTAMP_FORMAT ='YYYY-MM-DD HH24:MI:SS.FF'");
        connection.executeWithoutCommitting(
                "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT ='YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'");
        // This is necessary so that TIMESTAMP WITH LOCAL TIME ZONE get returned in UTC
        connection.executeWithoutCommitting("ALTER SESSION SET TIME_ZONE = '00:00'");
    }

    static void checkSupplementalLogging(JdbcConnection connection) throws SQLException {
        connection.query(
                "SELECT PARA_NAME, PARA_VALUE FROM V$DM_INI WHERE PARA_NAME IN ('ARCH_INI','RLOG_APPEND_LOGIC')",
                rs -> {
                    Map<String, String> params = new HashMap<>();
                    while (rs.next()) {
                        params.put(rs.getString(1), rs.getString(2));
                    }
                    if (!params.containsKey("ARCH_INI") || !params.get("ARCH_INI").equals("1")) {
                        throw new DebeziumException("ArchiveLog logging is not enabled");
                    }
                    if (!params.containsKey("RLOG_APPEND_LOGIC")
                            || !params.get("RLOG_APPEND_LOGIC").equals("2")) {
                        throw new DebeziumException(
                                "Supplemental logging is not enabled for all columns");
                    }
                });
    }

    public static List<LogFile> getLogFilesForOffsetScn(
            DamengConnection connection, Scn offsetScn, Duration archiveLogRetention)
            throws SQLException {
        LOGGER.trace("Getting logs to be mined for offset scn {}", offsetScn);

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT NAME, FIRST_CHANGE#, NEXT_CHANGE#, SEQUENCE# FROM V$ARCHIVED_LOG ");
        sb.append("WHERE ");
        sb.append("NAME IS NOT NULL AND STATUS='A' ");
        if (!archiveLogRetention.isNegative() && !archiveLogRetention.isZero()) {
            sb.append("AND A.FIRST_TIME >= SYSDATE - (")
                    .append(archiveLogRetention.toHours())
                    .append("/24) ");
        }
        sb.append("ORDER BY SEQUENCE#");

        List<LogFile> logFiles = new ArrayList<>();
        List<LogFile> archivedLogFiles = new ArrayList<>();
        connection.query(
                sb.toString(),
                rs -> {
                    while (rs.next()) {
                        String fileName = rs.getString(1);
                        Scn firstScn = getScnFromString(rs.getString(2));
                        Scn nextScn = getScnFromString(rs.getString(3));
                        Long sequence = rs.getLong(4);
                        LogFile logFile = new LogFile(fileName, firstScn, nextScn, sequence);
                        if (logFile.getNextScn().compareTo(offsetScn) >= 0) {
                            LOGGER.trace(
                                    "Archive log {} with SCN range {} to {} sequence {} to be added.",
                                    fileName,
                                    firstScn,
                                    nextScn,
                                    sequence);
                            archivedLogFiles.add(logFile);
                        }
                    }
                });
        logFiles.addAll(archivedLogFiles);

        return logFiles;
    }

    static Set<String> getCurrentLogFiles(DamengConnection connection) throws SQLException {
        final Set<String> fileNames = new HashSet<>();
        connection.query(
                "SELECT NAME FROM V$ARCHIVED_LOG",
                rs -> {
                    while (rs.next()) {
                        fileNames.add(rs.getString(1));
                    }
                });
        LOGGER.trace(" Current log fileNames: {} ", fileNames);
        return fileNames;
    }

    static Scn getEndScn(
            DamengConnection connection,
            Scn startScn,
            Scn prevEndScn,
            DamengStreamingChangeEventSourceMetrics streamingMetrics,
            int defaultBatchSize,
            boolean lobEnabled)
            throws SQLException {
        Scn currentScn = connection.getMaxArchiveLogScn();
        streamingMetrics.setCurrentScn(currentScn);

        Scn topScnToMine = startScn.add(Scn.valueOf(streamingMetrics.getBatchSize()));

        // adjust batch size
        boolean topMiningScnInFarFuture = false;
        if (topScnToMine.subtract(currentScn).compareTo(Scn.valueOf(defaultBatchSize)) > 0) {
            streamingMetrics.changeBatchSize(false, lobEnabled);
            topMiningScnInFarFuture = true;
        }
        if (currentScn.subtract(topScnToMine).compareTo(Scn.valueOf(defaultBatchSize)) > 0) {
            streamingMetrics.changeBatchSize(true, lobEnabled);
        }

        // adjust sleeping time to reduce DB impact
        if (currentScn.compareTo(topScnToMine) < 0) {
            if (!topMiningScnInFarFuture) {
                streamingMetrics.changeSleepingTime(true);
            }
            LOGGER.debug("Using current SCN {} as end SCN.", currentScn);
            return currentScn;
        } else {
            if (prevEndScn != null && topScnToMine.compareTo(prevEndScn) <= 0) {
                LOGGER.debug(
                        "Max batch size too small, using current SCN {} as end SCN.", currentScn);
                return currentScn;
            }
            streamingMetrics.changeSleepingTime(false);
            if (topScnToMine.compareTo(startScn) < 0) {
                LOGGER.debug(
                        "Top SCN calculation resulted in end before start SCN, using current SCN {} as end SCN.",
                        currentScn);
                return currentScn;
            }
            LOGGER.debug("Using Top SCN calculation {} as end SCN.", topScnToMine);
            return topScnToMine;
        }
    }

    public static void endMining(DamengConnection connection) {
        String stopMining = "BEGIN SYS.DBMS_LOGMNR.END_LOGMNR(); END;";
        try {
            executeCallableStatement(connection, stopMining);
        } catch (SQLException e) {
            if (e.getMessage().contains("-2846: DBMS_LOGMNR.END_LOGMNR")) {
                LOGGER.info("LogMiner session was already closed");
            } else {
                LOGGER.error("Cannot close LogMiner session gracefully: {}", e);
            }
        }
    }

    static void startLogMining(
            DamengConnection connection,
            Scn startScn,
            Scn endScn,
            DamengStreamingChangeEventSourceMetrics streamingMetrics)
            throws SQLException {
        LOGGER.trace("Starting log mining startScn={}, endScn={}", startScn, endScn);

        StringBuilder sb = new StringBuilder();
        sb.append("BEGIN ");
        sb.append("sys.dbms_logmnr.start_logmnr(");
        sb.append("STARTSCN => ").append(startScn.toString());
        sb.append(",");
        sb.append("ENDSCN => ").append(endScn.toString());
        sb.append(",");
        sb.append("OPTIONS => ").append("2128");
        sb.append("); ");
        sb.append("END;");

        String statement = sb.toString();
        try {
            Instant start = Instant.now();
            executeCallableStatement(connection, statement);
            streamingMetrics.addCurrentMiningSessionStart(Duration.between(start, Instant.now()));
        } catch (SQLException e) {
            logDatabaseState(connection);
            throw e;
        }
    }

    public static void setLogFilesForMining(
            DamengConnection connection,
            Scn lastProcessedScn,
            Duration archiveLogRetention,
            boolean archiveLogOnlyMode)
            throws SQLException {
        removeLogFilesFromMining(connection);

        List<LogFile> logFilesForMining =
                getLogFilesForOffsetScn(connection, lastProcessedScn, archiveLogRetention);
        if (!logFilesForMining.stream()
                .anyMatch(l -> l.getFirstScn().compareTo(lastProcessedScn) <= 0)) {
            Scn minScn =
                    logFilesForMining.stream()
                            .map(LogFile::getFirstScn)
                            .min(Scn::compareTo)
                            .orElse(Scn.NULL);

            if ((minScn.isNull() || logFilesForMining.isEmpty()) && archiveLogOnlyMode) {
                throw new DebeziumException(
                        "The log.mining.archive.log.only mode was recently enabled and the offset SCN "
                                + lastProcessedScn
                                + "is not yet in any available archive logs. "
                                + "Please perform an Oracle log switch and restart the connector.");
            }
            throw new IllegalStateException(
                    "None of log files contains offset SCN: "
                            + lastProcessedScn
                            + ", re-snapshot is required.");
        }

        List<String> logFilesNames =
                logFilesForMining.stream().map(LogFile::getName).collect(Collectors.toList());
        for (String file : logFilesNames) {
            LOGGER.trace("Adding log file {} to mining session", file);
            String addLogFileStatement = addLogFileStatement("DBMS_LOGMNR.ADDFILE", file);
            executeCallableStatement(connection, addLogFileStatement);
        }

        LOGGER.debug(
                "Last mined SCN: {}, Log file list to mine: {}\n", lastProcessedScn, logFilesNames);
    }

    static String addLogFileStatement(String option, String fileName) {
        return "BEGIN sys.dbms_logmnr.add_logfile(LOGFILENAME => '"
                + fileName
                + "', OPTIONS => "
                + option
                + ");END;";
    }

    public static void removeLogFilesFromMining(DamengConnection conn) throws SQLException {
        try (PreparedStatement ps =
                        conn.connection(false)
                                .prepareStatement("SELECT FILENAME AS NAME FROM V$LOGMNR_LOGS");
                ResultSet result = ps.executeQuery()) {
            Set<String> files = new LinkedHashSet<>();
            while (result.next()) {
                files.add(result.getString(1));
            }
            for (String fileName : files) {
                executeCallableStatement(
                        conn,
                        "BEGIN SYS.DBMS_LOGMNR.REMOVE_LOGFILE(LOGFILENAME => '"
                                + fileName
                                + "');END;");
                LOGGER.debug("File {} was removed from mining", fileName);
            }
        }
    }

    static Scn getFirstOnlineLogScn(DamengConnection connection, Duration archiveLogRetention)
            throws SQLException {
        LOGGER.trace("Getting first scn of all online logs");

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# FROM V$ARCHIVED_LOG ");
        sb.append("WHERE STATUS='A' ");
        if (!archiveLogRetention.isNegative() && !archiveLogRetention.isZero()) {
            sb.append("AND FIRST_TIME >= SYSDATE - (")
                    .append(archiveLogRetention.toHours())
                    .append("/24)");
        }

        try (Statement s = connection.connection(false).createStatement()) {
            try (ResultSet rs = s.executeQuery(sb.toString())) {
                rs.next();
                Scn firstScnOfOnlineLog = Scn.valueOf(rs.getString(1));
                LOGGER.trace("First SCN in online logs is {}", firstScnOfOnlineLog);
                return firstScnOfOnlineLog;
            }
        }
    }

    public static Optional<Scn> getLastScnToAbandon(
            DamengConnection connection, Scn offsetScn, Duration transactionRetention) {
        try {
            String query = diffInDaysQuery(offsetScn);
            Float diffInDays = (Float) getSingleResult(connection, query, DATATYPE.FLOAT);
            if (diffInDays != null && (diffInDays * 24) > transactionRetention.toHours()) {
                return Optional.of(offsetScn);
            }
            return Optional.empty();
        } catch (SQLException e) {
            LOGGER.error("Cannot calculate days difference due to {}", e);
            return Optional.of(offsetScn);
        }
    }

    public static String diffInDaysQuery(Scn scn) {
        if (scn == null) {
            return null;
        }
        return "select sysdate - CAST(scn_to_timestamp(" + scn.toString() + ") as date) from dual";
    }

    public static Object getSingleResult(DamengConnection connection, String query, DATATYPE type)
            throws SQLException {
        try (PreparedStatement statement = connection.connection(false).prepareStatement(query);
                ResultSet rs = statement.executeQuery()) {
            if (rs.next()) {
                switch (type) {
                    case LONG:
                        return rs.getLong(1);
                    case TIMESTAMP:
                        return rs.getTimestamp(1);
                    case STRING:
                        return rs.getString(1);
                    case FLOAT:
                        return rs.getFloat(1);
                }
            }
            return null;
        }
    }

    private static void logDatabaseState(DamengConnection connection) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Available archive logs are:");
            try {
                logQueryResults(connection, "SELECT * FROM V$ARCHIVED_LOG");
            } catch (SQLException e) {
                LOGGER.debug("Failed to obtain archive log table entries", e);
            }
            LOGGER.debug("Log history last 24 hours:");
            try {
                logQueryResults(
                        connection, "SELECT * FROM V$LOG_HISTORY WHERE FIRST_TIME >= SYSDATE - 1");
            } catch (SQLException e) {
                LOGGER.debug("Failed to obtain log history", e);
            }
            LOGGER.debug("Log entries registered with LogMiner are:");
            try {
                logQueryResults(connection, "SELECT * FROM V$LOGMNR_LOGS");
            } catch (SQLException e) {
                LOGGER.debug("Failed to obtain registered logs with LogMiner", e);
            }
            LOGGER.debug("Log mining session parameters are:");
            try {
                logQueryResults(connection, "SELECT * FROM V$LOGMNR_PARAMETERS");
            } catch (SQLException e) {
                LOGGER.debug("Failed to obtain log mining session parameters", e);
            }
        }
    }

    private static void logQueryResults(DamengConnection connection, String query)
            throws SQLException {
        connection.query(
                query,
                rs -> {
                    int columns = rs.getMetaData().getColumnCount();
                    List<String> columnNames = new ArrayList<>();
                    for (int index = 1; index <= columns; ++index) {
                        columnNames.add(rs.getMetaData().getColumnName(index));
                    }
                    LOGGER.debug("{}", columnNames);
                    while (rs.next()) {
                        List<Object> columnValues = new ArrayList<>();
                        for (int index = 1; index <= columns; ++index) {
                            columnValues.add(rs.getObject(index));
                        }
                        LOGGER.debug("{}", columnValues);
                    }
                });
    }

    static void logWarn(
            DamengStreamingChangeEventSourceMetrics streamingMetrics,
            String format,
            Object... args) {
        LOGGER.warn(format, args);
        streamingMetrics.incrementWarningCount();
    }

    static void logError(
            DamengStreamingChangeEventSourceMetrics streamingMetrics,
            String format,
            Object... args) {
        LOGGER.error(format, args);
        streamingMetrics.incrementErrorCount();
    }

    static OffsetDateTime getSystime(DamengConnection connection) throws SQLException {
        return connection.queryAndMap(
                "SELECT SYSTIMESTAMP FROM DUAL",
                rs -> {
                    if (rs.next()) {
                        return rs.getObject(1, OffsetDateTime.class);
                    } else {
                        return null;
                    }
                });
    }

    static List<BigInteger> getCurrentLogSequences(DamengConnection connection)
            throws SQLException {
        return connection.queryAndMap(
                "SELECT SEQUENCE# SEQUENCE# FROM V$ARCHIVED_LOG",
                rs -> {
                    List<BigInteger> sequences = new ArrayList<>();
                    while (rs.next()) {
                        sequences.add(new BigInteger(rs.getString(1)));
                    }
                    return sequences;
                });
    }

    private static Scn getScnFromString(String value) {
        if (Strings.isNullOrEmpty(value)) {
            return Scn.MAX;
        }
        return Scn.valueOf(value);
    }

    private static void executeCallableStatement(DamengConnection connection, String statement)
            throws SQLException {
        Objects.requireNonNull(statement);
        try (CallableStatement s = connection.connection(false).prepareCall(statement)) {
            s.execute();
        }
    }

    /**
     * Returns a 0-based index offset for the column name in the relational table.
     *
     * @param columnName the column name, should not be {@code null}.
     * @param table the relational table, should not be {@code null}.
     * @return the 0-based index offset for the column name
     */
    public static int getColumnIndexByName(String columnName, Table table) {
        final Column column = table.columnWithName(columnName);
        if (column == null) {
            throw new DebeziumException(
                    "No column '" + columnName + "' found in table '" + table.id() + "'");
        }
        // want to return a 0-based index and column positions are 1-based
        return column.position() - 1;
    }
}
