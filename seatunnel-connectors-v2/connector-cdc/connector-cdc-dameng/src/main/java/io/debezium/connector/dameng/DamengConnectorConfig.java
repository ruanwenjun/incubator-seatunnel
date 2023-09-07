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

package io.debezium.connector.dameng;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.dameng.logminer.HistoryRecorder;
import io.debezium.connector.dameng.logminer.NeverHistoryRecorder;
import io.debezium.document.Document;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.HistoryRecordComparator;
import lombok.Getter;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Getter
@SuppressWarnings("MagicNumber")
public class DamengConnectorConfig extends HistorizedRelationalDatabaseConnectorConfig {
    protected static final int DEFAULT_PORT = 5236;

    protected static final int DEFAULT_VIEW_FETCH_SIZE = 10_000;
    protected static final int DEFAULT_BATCH_SIZE = 20_000;
    protected static final int MIN_BATCH_SIZE = 1_000;
    protected static final int MAX_BATCH_SIZE = 100_000;
    protected static final Duration MAX_SLEEP_TIME = Duration.ofMillis(3_000);
    protected static final Duration DEFAULT_SLEEP_TIME = Duration.ofMillis(1_000);
    protected static final Duration MIN_SLEEP_TIME = Duration.ZERO;
    protected static final Duration SLEEP_TIME_INCREMENT = Duration.ofMillis(200);
    protected static final Duration ARCHIVE_LOG_ONLY_POLL_TIME = Duration.ofMillis(10_000);

    public static final Field PORT =
            RelationalDatabaseConnectorConfig.PORT.withDefault(DEFAULT_PORT);

    public static final Field HOSTNAME =
            RelationalDatabaseConnectorConfig.HOSTNAME
                    .withNoValidation()
                    .withValidation(DamengConnectorConfig::requiredWhenNoUrl);

    public static final Field URL =
            Field.create(DATABASE_CONFIG_PREFIX + "url")
                    .withDisplayName("Complete JDBC URL")
                    .withType(ConfigDef.Type.STRING)
                    .withWidth(ConfigDef.Width.LONG)
                    .withImportance(ConfigDef.Importance.HIGH)
                    .withValidation(DamengConnectorConfig::requiredWhenNoHostname)
                    .withDescription(
                            "Complete JDBC URL as an alternative to specifying hostname, port and database provided "
                                    + "as a way to support alternative connection scenarios.");

    public static final Field SNAPSHOT_MODE =
            Field.create("snapshot.mode")
                    .withDisplayName("Snapshot mode")
                    .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.LOW)
                    .withDescription(
                            "The criteria for running a snapshot upon startup of the connector. "
                                    + "Options include: "
                                    + "'initial' (the default) to specify the connector should run a snapshot only when no offsets are available for the logical server name; "
                                    + "'schema_only' to specify the connector should run a snapshot of the schema when no offsets are available for the logical server name. ");

    public static final Field SNAPSHOT_LOCKING_MODE =
            Field.create("snapshot.locking.mode")
                    .withDisplayName("Snapshot locking mode")
                    .withEnum(SnapshotLockingMode.class, SnapshotLockingMode.SHARED)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.LOW)
                    .withDescription(
                            "Controls how the connector holds locks on tables while performing the schema snapshot. The default is 'shared', "
                                    + "which means the connector will hold a table lock that prevents exclusive table access for just the initial portion of the snapshot "
                                    + "while the database schemas and other metadata are being read. The remaining work in a snapshot involves selecting all rows from "
                                    + "each table, and this is done using a flashback query that requires no locks. However, in some cases it may be desirable to avoid "
                                    + "locks entirely which can be done by specifying 'none'. This mode is only safe to use if no schema changes are happening while the "
                                    + "snapshot is taken.");
    public static final Field SERVER_NAME =
            RelationalDatabaseConnectorConfig.SERVER_NAME.withValidation(
                    CommonConnectorConfig::validateServerNameIsDifferentFromHistoryTopicName);

    public static final Field LOB_ENABLED =
            Field.create("lob.enabled")
                    .withDisplayName(
                            "Specifies whether the connector supports mining LOB fields and operations")
                    .withType(ConfigDef.Type.BOOLEAN)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.LOW)
                    .withDefault(false)
                    .withDescription(
                            "When set to `false`, the default, LOB fields will not be captured nor emitted. When set to `true`, the connector "
                                    + "will capture LOB fields and emit changes for those fields like any other column type.");

    public static final Field LOG_MINING_STRATEGY =
            Field.create("log.mining.strategy")
                    .withDisplayName("Log Mining Strategy")
                    .withEnum(LogMiningStrategy.class, LogMiningStrategy.CATALOG_IN_REDO)
                    .withWidth(ConfigDef.Width.MEDIUM)
                    .withImportance(ConfigDef.Importance.HIGH)
                    .withDescription(
                            "There are strategies: Online catalog with faster mining but no captured DDL. Another - with data dictionary loaded into REDO LOG files");

    // this option could be true up to Oracle 18c version. Starting from Oracle 19c this option
    // cannot be true todo should we do it?
    public static final Field CONTINUOUS_MINE =
            Field.create("log.mining.continuous.mine")
                    .withDisplayName(
                            "Should log mining session configured with CONTINUOUS_MINE setting?")
                    .withType(ConfigDef.Type.BOOLEAN)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.LOW)
                    .withDefault(false)
                    .withValidation(Field::isBoolean)
                    .withDescription(
                            "If true, CONTINUOUS_MINE option will be added to the log mining session. This will manage log files switches seamlessly.");

    public static final Field SNAPSHOT_ENHANCEMENT_TOKEN =
            Field.create("snapshot.enhance.predicate.scn")
                    .withDisplayName("A string to replace on snapshot predicate enhancement")
                    .withType(ConfigDef.Type.STRING)
                    .withWidth(ConfigDef.Width.MEDIUM)
                    .withImportance(ConfigDef.Importance.HIGH)
                    .withDescription("A token to replace on snapshot predicate template");

    @Deprecated
    public static final Field LOG_MINING_HISTORY_RECORDER_CLASS =
            Field.createInternal("log.mining.history.recorder.class")
                    .withDisplayName("Log Mining History Recorder Class")
                    .withType(ConfigDef.Type.STRING)
                    .withWidth(ConfigDef.Width.MEDIUM)
                    .withImportance(ConfigDef.Importance.MEDIUM)
                    .withInvisibleRecommender()
                    .withDescription(
                            "(Deprecated) Allows connector deployment to capture log mining results");

    @Deprecated
    public static final Field LOG_MINING_HISTORY_RETENTION =
            Field.createInternal("log.mining.history.retention.hours")
                    .withDisplayName("Log Mining history retention")
                    .withType(ConfigDef.Type.LONG)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.MEDIUM)
                    .withDefault(0)
                    .withDescription(
                            "(Deprecated) When supplying a log.mining.history.recorder.class, this option specifies the number of hours "
                                    + "the recorder should keep the history.  The default, 0, indicates that no history should be retained.");

    public static final Field LOG_MINING_TRANSACTION_RETENTION =
            Field.create("log.mining.transaction.retention.hours")
                    .withDisplayName("Log Mining long running transaction retention")
                    .withType(ConfigDef.Type.LONG)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.MEDIUM)
                    .withDefault(0)
                    .withValidation(Field::isNonNegativeInteger)
                    .withDescription(
                            "Hours to keep long running transactions in transaction buffer between log mining sessions.  By default, all transactions are retained.");

    public static final Field LOG_MINING_ARCHIVE_LOG_HOURS =
            Field.create("log.mining.archive.log.hours")
                    .withDisplayName("Log Mining Archive Log Hours")
                    .withType(ConfigDef.Type.LONG)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.LOW)
                    .withDefault(0)
                    .withDescription(
                            "The number of hours in the past from SYSDATE to mine archive logs.  Using 0 mines all available archive logs");

    public static final Field LOG_MINING_BATCH_SIZE_MIN =
            Field.create("log.mining.batch.size.min")
                    .withDisplayName("Minimum batch size for reading redo/archive logs.")
                    .withType(ConfigDef.Type.LONG)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.LOW)
                    .withDefault(MIN_BATCH_SIZE)
                    .withDescription(
                            "The minimum SCN interval size that this connector will try to read from redo/archive logs. Active batch size will be also increased/decreased by this amount for tuning connector throughput when needed.");

    public static final Field LOG_MINING_BATCH_SIZE_DEFAULT =
            Field.create("log.mining.batch.size.default")
                    .withDisplayName("Default batch size for reading redo/archive logs.")
                    .withType(ConfigDef.Type.LONG)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.LOW)
                    .withDefault(DEFAULT_BATCH_SIZE)
                    .withDescription(
                            "The starting SCN interval size that the connector will use for reading data from redo/archive logs.");

    public static final Field LOG_MINING_BATCH_SIZE_MAX =
            Field.create("log.mining.batch.size.max")
                    .withDisplayName("Maximum batch size for reading redo/archive logs.")
                    .withType(ConfigDef.Type.LONG)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.LOW)
                    .withDefault(MAX_BATCH_SIZE)
                    .withDescription(
                            "The maximum SCN interval size that this connector will use when reading from redo/archive logs.");

    public static final Field LOG_MINING_VIEW_FETCH_SIZE =
            Field.create("log.mining.view.fetch.size")
                    .withDisplayName("Number of content records that will be fetched.")
                    .withType(ConfigDef.Type.LONG)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.LOW)
                    .withDefault(DEFAULT_VIEW_FETCH_SIZE)
                    .withDescription(
                            "The number of content records that will be fetched from the LogMiner content view.");

    public static final Field LOG_MINING_SLEEP_TIME_MIN_MS =
            Field.create("log.mining.sleep.time.min.ms")
                    .withDisplayName(
                            "Minimum sleep time in milliseconds when reading redo/archive logs.")
                    .withType(ConfigDef.Type.LONG)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.LOW)
                    .withDefault(MIN_SLEEP_TIME.toMillis())
                    .withDescription(
                            "The minimum amount of time that the connector will sleep after reading data from redo/archive logs and before starting reading data again. Value is in milliseconds.");

    public static final Field LOG_MINING_SLEEP_TIME_DEFAULT_MS =
            Field.create("log.mining.sleep.time.default.ms")
                    .withDisplayName(
                            "Default sleep time in milliseconds when reading redo/archive logs.")
                    .withType(ConfigDef.Type.LONG)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.LOW)
                    .withDefault(DEFAULT_SLEEP_TIME.toMillis())
                    .withDescription(
                            "The amount of time that the connector will sleep after reading data from redo/archive logs and before starting reading data again. Value is in milliseconds.");

    public static final Field LOG_MINING_SLEEP_TIME_MAX_MS =
            Field.create("log.mining.sleep.time.max.ms")
                    .withDisplayName(
                            "Maximum sleep time in milliseconds when reading redo/archive logs.")
                    .withType(ConfigDef.Type.LONG)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.LOW)
                    .withDefault(MAX_SLEEP_TIME.toMillis())
                    .withDescription(
                            "The maximum amount of time that the connector will sleep after reading data from redo/archive logs and before starting reading data again. Value is in milliseconds.");

    public static final Field LOG_MINING_SLEEP_TIME_INCREMENT_MS =
            Field.create("log.mining.sleep.time.increment.ms")
                    .withDisplayName(
                            "The increment in sleep time in milliseconds used to tune auto-sleep behavior.")
                    .withType(ConfigDef.Type.LONG)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.LOW)
                    .withDefault(SLEEP_TIME_INCREMENT.toMillis())
                    .withDescription(
                            "The maximum amount of time that the connector will use to tune the optimal sleep time when reading data from LogMiner. Value is in milliseconds.");

    public static final Field LOG_MINING_ARCHIVE_LOG_ONLY_SCN_POLL_INTERVAL_MS =
            Field.create("log.mining.archive.log.only.scn.poll.interval.ms")
                    .withDisplayName(
                            "The interval in milliseconds to wait between polls when SCN is not yet in the archive logs")
                    .withType(ConfigDef.Type.LONG)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.LOW)
                    .withDefault(ARCHIVE_LOG_ONLY_POLL_TIME.toMillis())
                    .withDescription(
                            "The interval in milliseconds to wait between polls checking to see if the SCN is in the archive logs.");

    private static final ConfigDefinition CONFIG_DEFINITION =
            HistorizedRelationalDatabaseConnectorConfig.CONFIG_DEFINITION
                    .edit()
                    .name("Dameng")
                    .excluding(
                            SCHEMA_WHITELIST,
                            SCHEMA_INCLUDE_LIST,
                            SCHEMA_BLACKLIST,
                            SCHEMA_EXCLUDE_LIST,
                            RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN,
                            SERVER_NAME)
                    .type(
                            HOSTNAME,
                            PORT,
                            USER,
                            PASSWORD,
                            SERVER_NAME,
                            DATABASE_NAME,
                            SNAPSHOT_MODE,
                            LOG_MINING_STRATEGY,
                            URL)
                    .connector(
                            SNAPSHOT_ENHANCEMENT_TOKEN,
                            SNAPSHOT_LOCKING_MODE,
                            LOG_MINING_HISTORY_RECORDER_CLASS,
                            LOG_MINING_HISTORY_RETENTION,
                            LOG_MINING_ARCHIVE_LOG_HOURS,
                            LOG_MINING_BATCH_SIZE_DEFAULT,
                            LOG_MINING_BATCH_SIZE_MIN,
                            LOG_MINING_BATCH_SIZE_MAX,
                            LOG_MINING_SLEEP_TIME_DEFAULT_MS,
                            LOG_MINING_SLEEP_TIME_MIN_MS,
                            LOG_MINING_SLEEP_TIME_MAX_MS,
                            LOG_MINING_SLEEP_TIME_INCREMENT_MS,
                            LOG_MINING_TRANSACTION_RETENTION,
                            LOB_ENABLED,
                            LOG_MINING_ARCHIVE_LOG_ONLY_SCN_POLL_INTERVAL_MS)
                    .create();

    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    public static final List<String> EXCLUDED_SCHEMAS =
            Collections.unmodifiableList(
                    Arrays.asList("sys", "sysdba", "syssso", "sysauditor", "ctisys"));

    private final String databaseName;
    private final SnapshotMode snapshotMode;
    private final SnapshotLockingMode snapshotLockingMode;
    private final HistoryRecorder logMiningHistoryRecorder;

    // LogMiner options
    private final LogMiningStrategy logMiningStrategy;
    private final long logMiningHistoryRetentionHours;
    private final boolean logMiningContinuousMine;
    private final Duration logMiningArchiveLogRetention;
    private final int logMiningBatchSizeMin;
    private final int logMiningBatchSizeMax;
    private final int logMiningBatchSizeDefault;
    private final int logMiningViewFetchSize;
    private final Duration logMiningSleepTimeMin;
    private final Duration logMiningSleepTimeMax;
    private final Duration logMiningSleepTimeDefault;
    private final Duration logMiningSleepTimeIncrement;
    private final Duration logMiningTransactionRetention;
    private final boolean lobEnabled;
    private final Duration archiveLogOnlyScnPollTime;

    public DamengConnectorConfig(Configuration config) {
        super(
                DamengConnector.class,
                config,
                config.getString(SERVER_NAME),
                new SystemTablesPredicate(),
                x -> x.schema() + "." + x.table(),
                true,
                ColumnFilterMode.SCHEMA);

        this.databaseName = toUpperCase(config.getString(DATABASE_NAME));
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE));
        this.snapshotLockingMode =
                SnapshotLockingMode.parse(
                        config.getString(SNAPSHOT_LOCKING_MODE),
                        SNAPSHOT_LOCKING_MODE.defaultValueAsString());
        this.logMiningHistoryRecorder = resolveLogMiningHistoryRecorder(config);

        // LogMiner
        this.lobEnabled = config.getBoolean(LOB_ENABLED);
        this.logMiningStrategy = LogMiningStrategy.parse(config.getString(LOG_MINING_STRATEGY));
        this.logMiningHistoryRetentionHours = config.getLong(LOG_MINING_HISTORY_RETENTION);
        this.logMiningContinuousMine = config.getBoolean(CONTINUOUS_MINE);
        this.logMiningArchiveLogRetention =
                Duration.ofHours(config.getLong(LOG_MINING_ARCHIVE_LOG_HOURS));
        this.logMiningBatchSizeMin = config.getInteger(LOG_MINING_BATCH_SIZE_MIN);
        this.logMiningBatchSizeMax = config.getInteger(LOG_MINING_BATCH_SIZE_MAX);
        this.logMiningBatchSizeDefault = config.getInteger(LOG_MINING_BATCH_SIZE_DEFAULT);
        this.logMiningViewFetchSize = config.getInteger(LOG_MINING_VIEW_FETCH_SIZE);
        this.logMiningSleepTimeMin =
                Duration.ofMillis(config.getInteger(LOG_MINING_SLEEP_TIME_MIN_MS));
        this.logMiningSleepTimeMax =
                Duration.ofMillis(config.getInteger(LOG_MINING_SLEEP_TIME_MAX_MS));
        this.logMiningSleepTimeDefault =
                Duration.ofMillis(config.getInteger(LOG_MINING_SLEEP_TIME_DEFAULT_MS));
        this.logMiningSleepTimeIncrement =
                Duration.ofMillis(config.getInteger(LOG_MINING_SLEEP_TIME_INCREMENT_MS));
        this.logMiningTransactionRetention =
                Duration.ofHours(config.getInteger(LOG_MINING_TRANSACTION_RETENTION));
        this.archiveLogOnlyScnPollTime =
                Duration.ofMillis(
                        config.getInteger(LOG_MINING_ARCHIVE_LOG_ONLY_SCN_POLL_INTERVAL_MS));
    }

    @Override
    protected HistoryRecordComparator getHistoryRecordComparator() {
        return new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document recorded, Document desired) {
                return Scn.valueOf(recorded.getString(SourceInfo.SCN_KEY))
                                .compareTo(Scn.valueOf(desired.getString(SourceInfo.SCN_KEY)))
                        < 1;
            }
        };
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    public Boolean isLogMiningHistoryRecorded() {
        return logMiningHistoryRetentionHours > 0;
    }

    public String getCatalogName() {
        return getDatabaseName();
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(
            Version version) {
        return new DamengSourceInfoStructMaker(Module.name(), Module.version(), this);
    }

    private static String toUpperCase(String property) {
        return property == null ? null : property.toUpperCase();
    }

    public static int requiredWhenNoHostname(
            Configuration config, Field field, Field.ValidationOutput problems) {
        // Validates that the field is required but only when an URL field is not present
        if (config.getString(HOSTNAME) == null) {
            return Field.isRequired(config, field, problems);
        }
        return 0;
    }

    private static class SystemTablesPredicate implements Tables.TableFilter {

        @Override
        public boolean isIncluded(TableId t) {
            return !EXCLUDED_SCHEMAS.contains(t.schema().toLowerCase());
        }
    }

    public enum SnapshotMode implements EnumeratedValue {

        /** Perform a snapshot of data and schema upon initial startup of a connector. */
        INITIAL("initial", true),

        /** Perform a snapshot of the schema but no data upon initial startup of a connector. */
        SCHEMA_ONLY("schema_only", false);

        private final String value;
        private final boolean includeData;

        private SnapshotMode(String value, boolean includeData) {
            this.value = value;
            this.includeData = includeData;
        }

        @Override
        public String getValue() {
            return value;
        }

        public boolean includeData() {
            return includeData;
        }

        public static SnapshotMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();

            for (SnapshotMode option : SnapshotMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }

            return null;
        }

        public static SnapshotMode parse(String value, String defaultValue) {
            SnapshotMode mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }
    }

    public enum SnapshotLockingMode implements EnumeratedValue {
        /**
         * This mode will allow concurrent access to the table during the snapshot but prevents any
         * session from acquiring any table-level exclusive lock.
         */
        SHARED("shared"),

        /**
         * This mode will avoid using ANY table locks during the snapshot process. This mode should
         * be used carefully only when no schema changes are to occur.
         */
        NONE("none");

        private final String value;

        private SnapshotLockingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public boolean usesLocking() {
            return !value.equals(NONE.value);
        }

        public static SnapshotLockingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotLockingMode option : SnapshotLockingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        public static SnapshotLockingMode parse(String value, String defaultValue) {
            SnapshotLockingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    public enum LogMiningStrategy implements EnumeratedValue {

        /**
         * This strategy uses LogMiner with data dictionary in online catalog. This option will not
         * capture DDL , but acts fast on REDO LOG switch events This option does not use
         * CONTINUOUS_MINE option
         */
        ONLINE_CATALOG("online_catalog"),

        /**
         * This strategy uses LogMiner with data dictionary in REDO LOG files. This option will
         * capture DDL, but will develop some lag on REDO LOG switch event and will eventually catch
         * up This option does not use CONTINUOUS_MINE option This is default value
         */
        CATALOG_IN_REDO("redo_log_catalog");

        private final String value;

        LogMiningStrategy(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static LogMiningStrategy parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (LogMiningStrategy adapter : LogMiningStrategy.values()) {
                if (adapter.getValue().equalsIgnoreCase(value)) {
                    return adapter;
                }
            }
            return null;
        }

        public static LogMiningStrategy parse(String value, String defaultValue) {
            LogMiningStrategy mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }
    }

    public static int requiredWhenNoUrl(
            Configuration config, Field field, Field.ValidationOutput problems) {

        // Validates that the field is required but only when an URL field is not present
        if (config.getString(URL) == null) {
            return Field.isRequired(config, field, problems);
        }
        return 0;
    }

    private static HistoryRecorder resolveLogMiningHistoryRecorder(Configuration config) {
        if (!config.hasKey(LOG_MINING_HISTORY_RECORDER_CLASS.name())) {
            return new NeverHistoryRecorder();
        }
        if (config.getLong(LOG_MINING_HISTORY_RETENTION) == 0L) {
            return new NeverHistoryRecorder();
        }
        return config.getInstance(LOG_MINING_HISTORY_RECORDER_CLASS, HistoryRecorder.class);
    }
}
