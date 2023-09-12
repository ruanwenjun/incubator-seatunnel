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

import org.apache.seatunnel.connectors.cdc.dameng.config.DamengSourceConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.dameng.DamengConnection;
import io.debezium.connector.dameng.DamengConnectorConfig;
import io.debezium.connector.dameng.DamengDatabaseSchema;
import io.debezium.connector.dameng.DamengOffsetContext;
import io.debezium.connector.dameng.DamengStreamingChangeEventSource;
import io.debezium.connector.dameng.DamengStreamingChangeEventSourceMetrics;
import io.debezium.connector.dameng.Scn;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Stopwatch;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.debezium.connector.dameng.logminer.LogMinerHelper.checkSupplementalLogging;
import static io.debezium.connector.dameng.logminer.LogMinerHelper.endMining;
import static io.debezium.connector.dameng.logminer.LogMinerHelper.getCurrentLogFiles;
import static io.debezium.connector.dameng.logminer.LogMinerHelper.getEndScn;
import static io.debezium.connector.dameng.logminer.LogMinerHelper.getFirstOnlineLogScn;
import static io.debezium.connector.dameng.logminer.LogMinerHelper.getLastScnToAbandon;
import static io.debezium.connector.dameng.logminer.LogMinerHelper.getLogFilesForOffsetScn;
import static io.debezium.connector.dameng.logminer.LogMinerHelper.getSystime;
import static io.debezium.connector.dameng.logminer.LogMinerHelper.logError;
import static io.debezium.connector.dameng.logminer.LogMinerHelper.setLogFilesForMining;
import static io.debezium.connector.dameng.logminer.LogMinerHelper.setNlsSessionParameters;
import static io.debezium.connector.dameng.logminer.LogMinerHelper.startLogMining;

public class LogMinerStreamingChangeEventSource extends DamengStreamingChangeEventSource {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(LogMinerStreamingChangeEventSource.class);

    private final DamengSourceConfig sourceConfig;
    private final DamengConnectorConfig connectorConfig;
    private final JdbcConfiguration jdbcConfiguration;
    private final String database;
    private final DamengConnection jdbcConnection;
    private final EventDispatcher<TableId> eventDispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final DamengDatabaseSchema databaseSchema;
    private final boolean archiveLogOnlyMode;
    private final boolean isContinuousMining;
    private final Duration archiveLogRetention;
    private final DamengStreamingChangeEventSourceMetrics streamingMetrics;

    private Scn startScn;
    private Scn endScn;
    private List<BigInteger> currentLogSequences;

    public LogMinerStreamingChangeEventSource(
            DamengSourceConfig sourceConfig,
            DamengConnection connection,
            EventDispatcher<TableId> eventDispatcher,
            ErrorHandler errorHandler,
            Clock clock,
            DamengDatabaseSchema databaseSchema,
            DamengStreamingChangeEventSourceMetrics streamingMetrics) {
        this.sourceConfig = sourceConfig;
        this.connectorConfig = sourceConfig.getDbzConnectorConfig();
        this.jdbcConfiguration =
                JdbcConfiguration.adapt(sourceConfig.getOriginDbzConnectorConfig());
        this.database = sourceConfig.getDbzConnectorConfig().getDatabaseName();
        this.jdbcConnection = connection;
        this.eventDispatcher = eventDispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.databaseSchema = databaseSchema;
        this.streamingMetrics = streamingMetrics;
        this.archiveLogOnlyMode = true;
        this.isContinuousMining = false;
        this.archiveLogRetention = connectorConfig.getLogMiningArchiveLogRetention();
    }

    @Override
    public void execute(
            final ChangeEventSourceContext context, final DamengOffsetContext offsetContext)
            throws InterruptedException {
        try (TransactionalBuffer transactionalBuffer =
                new TransactionalBuffer(
                        connectorConfig, databaseSchema, clock, errorHandler, streamingMetrics)) {
            try {
                startScn = offsetContext.getScn();
                Scn firstOnlineLogScn;
                if (!isContinuousMining
                        && startScn.compareTo(
                                        (firstOnlineLogScn =
                                                getFirstOnlineLogScn(
                                                        jdbcConnection, archiveLogRetention)))
                                < 0) {
                    LOGGER.warn(
                            "Online LOG files or archive log files do not contain the offset scn {}."
                                    + "Turn start scn to online log first scn: {}.",
                            startScn,
                            firstOnlineLogScn);
                    startScn = firstOnlineLogScn;
                }

                setNlsSessionParameters(jdbcConnection);
                checkSupplementalLogging(jdbcConnection);

                if (archiveLogOnlyMode && !waitForStartScnInArchiveLogs(context, startScn)) {
                    return;
                }

                try (HistoryRecorder historyRecorder =
                        connectorConfig.getLogMiningHistoryRecorder()) {
                    historyRecorder.prepare(
                            streamingMetrics,
                            jdbcConfiguration,
                            connectorConfig.getLogMiningHistoryRetentionHours());

                    LogMinerQueryResultProcessor processor =
                            new LogMinerQueryResultProcessor(
                                    context,
                                    connectorConfig,
                                    streamingMetrics,
                                    transactionalBuffer,
                                    offsetContext,
                                    databaseSchema,
                                    eventDispatcher,
                                    historyRecorder);

                    String query =
                            LogMinerQueryBuilder.build(
                                    connectorConfig, databaseSchema, jdbcConnection.username());
                    try (PreparedStatement miningView =
                            jdbcConnection
                                    .connection()
                                    .prepareStatement(
                                            query,
                                            ResultSet.TYPE_FORWARD_ONLY,
                                            ResultSet.CONCUR_READ_ONLY,
                                            ResultSet.HOLD_CURSORS_OVER_COMMIT)) {
                        Stopwatch stopwatch = Stopwatch.reusable();
                        currentLogSequences = getCurrentLogSequences();
                        while (context.isRunning()) {
                            streamingMetrics.calculateTimeDifference(getSystime(jdbcConnection));
                            if (archiveLogOnlyMode
                                    && !waitForStartScnInArchiveLogs(context, startScn)) {
                                break;
                            }

                            Instant start = Instant.now();
                            endScn =
                                    getEndScn(
                                            jdbcConnection,
                                            startScn,
                                            endScn,
                                            streamingMetrics,
                                            connectorConfig.getLogMiningBatchSizeDefault(),
                                            connectorConfig.isLobEnabled());
                            if (archiveLogOnlyMode && startScn.equals(endScn)) {
                                pauseBetweenMiningSessions();
                                continue;
                            }

                            if (hasLogSwitchOccurred()) {
                                LOGGER.trace(
                                        "Ending log mining startScn={}, endScn={}, offsetContext.getScn={}",
                                        startScn,
                                        endScn,
                                        offsetContext.getScn());
                                endMining(jdbcConnection);
                                initializeLogsForMining(jdbcConnection, true, startScn);

                                abandonOldTransactionsIfExist(
                                        jdbcConnection, offsetContext, transactionalBuffer);
                                currentLogSequences = getCurrentLogSequences();
                            } else {
                                endMining(jdbcConnection);
                                initializeLogsForMining(jdbcConnection, true, startScn);
                            }

                            startLogMining(jdbcConnection, startScn, endScn, streamingMetrics);
                            LOGGER.trace(
                                    "Fetching LogMiner view results SCN {} to {}",
                                    startScn,
                                    endScn);

                            stopwatch.start();
                            miningView.setFetchSize(connectorConfig.getMaxQueueSize());
                            miningView.setFetchDirection(ResultSet.FETCH_FORWARD);
                            miningView.setString(1, startScn.toString());
                            miningView.setString(2, endScn.toString());
                            try (ResultSet rs = miningView.executeQuery()) {
                                Duration lastDurationOfBatchCapturing =
                                        stopwatch.stop().durations().statistics().getTotal();
                                streamingMetrics.setLastDurationOfBatchCapturing(
                                        lastDurationOfBatchCapturing);
                                processor.processResult(rs);

                                if (connectorConfig.isLobEnabled()) {
                                    startScn =
                                            transactionalBuffer.updateOffsetContext(
                                                    offsetContext, eventDispatcher);
                                } else {
                                    final Scn lastProcessedScn = processor.getLastProcessedScn();
                                    if (!lastProcessedScn.isNull()
                                            && lastProcessedScn.compareTo(endScn) < 0) {
                                        endScn = lastProcessedScn;
                                    }
                                    if (transactionalBuffer.isEmpty()) {
                                        LOGGER.debug(
                                                "Buffer is empty, updating offset SCN to {}",
                                                endScn);
                                        offsetContext.setScn(endScn);
                                    } else {
                                        final Scn minStartScn = transactionalBuffer.getMinimumScn();
                                        if (!minStartScn.isNull()) {
                                            offsetContext.setScn(
                                                    minStartScn.subtract(Scn.valueOf(1)));
                                            eventDispatcher.dispatchHeartbeatEvent(offsetContext);
                                        }
                                    }
                                    startScn = endScn;
                                }
                            }

                            afterHandleScn(offsetContext);
                            streamingMetrics.setCurrentBatchProcessingTime(
                                    Duration.between(start, Instant.now()));
                            pauseBetweenMiningSessions();
                        }
                    }
                }
            } catch (Throwable t) {
                logError(streamingMetrics, "Mining session stopped due to the {}", t);
                errorHandler.setProducerThrowable(t);
            } finally {
                LOGGER.info(
                        "startScn={}, endScn={}, offsetContext.getScn()={}",
                        startScn,
                        endScn,
                        offsetContext.getScn());
                LOGGER.info("Transactional buffer dump: {}", transactionalBuffer.toString());
                LOGGER.info("Streaming metrics dump: {}", streamingMetrics.toString());
            }
        }
    }

    private boolean waitForStartScnInArchiveLogs(ChangeEventSourceContext context, Scn startScn)
            throws SQLException, InterruptedException {
        boolean showStartScnNotInArchiveLogs = true;
        while (context.isRunning() && !isStartScnInArchiveLogs(startScn)) {
            if (showStartScnNotInArchiveLogs) {
                LOGGER.warn(
                        "Starting SCN {} is not yet in archive logs, waiting for archive log switch.",
                        startScn);
                showStartScnNotInArchiveLogs = false;
                Metronome.sleeper(connectorConfig.getArchiveLogOnlyScnPollTime(), clock).pause();
            }
        }

        if (!context.isRunning()) {
            return false;
        }

        if (!showStartScnNotInArchiveLogs) {
            LOGGER.info(
                    "Starting SCN {} is now available in archive logs, log mining unpaused.",
                    startScn);
        }
        return true;
    }

    private boolean isStartScnInArchiveLogs(Scn startScn) throws SQLException {
        List<LogFile> logs = getLogFilesForOffsetScn(jdbcConnection, startScn, archiveLogRetention);
        return logs.stream()
                .anyMatch(
                        l ->
                                l.getFirstScn().compareTo(startScn) <= 0
                                        && l.getNextScn().compareTo(startScn) >= 0);
    }

    private void pauseBetweenMiningSessions() throws InterruptedException {
        Duration period =
                Duration.ofMillis(streamingMetrics.getMillisecondToSleepBetweenMiningQuery());
        Metronome.sleeper(period, clock).pause();
    }

    private boolean hasLogSwitchOccurred() throws SQLException {
        final List<BigInteger> newSequences = getCurrentLogSequences();
        if (!newSequences.equals(currentLogSequences)) {
            LOGGER.debug(
                    "Current log sequence(s) is now {}, was {}", newSequences, currentLogSequences);
            currentLogSequences = newSequences;

            // todo 处理 endscn 到期

            Set<String> fileNames = getCurrentLogFiles(jdbcConnection);
            streamingMetrics.setCurrentLogFileName(fileNames);
            return true;
        }

        return false;
    }

    private List<BigInteger> getCurrentLogSequences() throws SQLException {
        return LogMinerHelper.getCurrentLogSequences(jdbcConnection);
    }

    private void abandonOldTransactionsIfExist(
            DamengConnection connection,
            DamengOffsetContext offsetContext,
            TransactionalBuffer transactionalBuffer) {
        Duration transactionRetention = connectorConfig.getLogMiningTransactionRetention();
        if (!Duration.ZERO.equals(transactionRetention)) {
            final Scn offsetScn = offsetContext.getScn();
            Optional<Scn> lastScnToAbandonTransactions =
                    getLastScnToAbandon(connection, offsetScn, transactionRetention);
            lastScnToAbandonTransactions.ifPresent(
                    thresholdScn -> {
                        transactionalBuffer.abandonLongTransactions(thresholdScn, offsetContext);
                        offsetContext.setScn(thresholdScn);
                        startScn = endScn;
                    });
        }
    }

    private void initializeLogsForMining(
            DamengConnection connection, boolean postEndMiningSession, Scn startScn)
            throws SQLException {
        setLogFilesForMining(connection, startScn, archiveLogRetention, archiveLogOnlyMode);
    }

    protected void afterHandleScn(DamengOffsetContext offsetContext) {}
}
