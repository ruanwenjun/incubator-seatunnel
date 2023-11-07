package io.debezium.connector.oracle;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.config.Oracle9BridgeSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.utils.Oracle9BridgeClientUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.utils.OracleConnectionUtils;

import org.apache.commons.collections4.CollectionUtils;

import org.whaleops.whaletunnel.oracleagent.sdk.OracleAgentClient;
import org.whaleops.whaletunnel.oracleagent.sdk.OracleAgentClientFactory;
import org.whaleops.whaletunnel.oracleagent.sdk.model.OracleDDLOperation;
import org.whaleops.whaletunnel.oracleagent.sdk.model.OracleOperation;
import org.whaleops.whaletunnel.oracleagent.sdk.model.OracleTransactionData;
import org.whaleops.whaletunnel.oracleagent.sdk.model.OracleTransactionFileNumberFetchRequest;

import io.debezium.connector.oracle.oracle9bridge.Oracle9BridgeDmlEntry;
import io.debezium.connector.oracle.oracle9bridge.Oracle9BridgeDmlEntryFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import lombok.extern.slf4j.Slf4j;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class Oracle9BridgeStreamingChangeEventSource
        implements StreamingChangeEventSource<Oracle9BridgeOffsetContext> {

    private static final Long NO_DATA_AVAILABLE_SLEEP_MS = 5_000L;

    private final Oracle9BridgeConnectorConfig oracle9BridgeConnectorConfig;
    private final OracleValueConverters oracleValueConverters;
    private final Oracle9BridgeSourceConfig sourceConfig;
    private final JdbcSourceEventDispatcher eventDispatcher;
    private ChangeEventSourceContext context;
    private final OracleDatabaseSchema oracleDatabaseSchema;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    // todo: we don't support multiple database now, since the oracle9bridge event doesn't contains
    // the database field,
    // one oracle9bridge should only bind to one database instance.
    protected final Map<String, TableId> tableNameToIdMap;
    protected final List<String> tables;
    protected final List<String> tableOwners;

    public Oracle9BridgeStreamingChangeEventSource(
            Oracle9BridgeOffsetContext offsetContext,
            Oracle9BridgeConnectorConfig connectorConfig,
            OracleConnection oracleConnection,
            List<TableId> tableIds,
            Oracle9BridgeSourceConfig sourceConfig,
            JdbcSourceEventDispatcher eventDispatcher,
            ErrorHandler errorHandler,
            OracleDatabaseSchema oracleDatabaseSchema) {
        this.oracleValueConverters = new OracleValueConverters(connectorConfig, oracleConnection);
        this.oracle9BridgeConnectorConfig = connectorConfig;
        this.sourceConfig = sourceConfig;
        this.eventDispatcher = eventDispatcher;
        this.errorHandler = errorHandler;
        this.clock = Clock.system();
        this.oracleDatabaseSchema = oracleDatabaseSchema;
        tableNameToIdMap =
                tableIds.stream().collect(Collectors.toMap(TableId::table, Function.identity()));
        this.tables = tableIds.stream().map(TableId::table).collect(Collectors.toList());
        this.tableOwners =
                tableNameToIdMap.keySet().stream()
                        .map(table -> OracleConnectionUtils.getTableOwner(oracleConnection, table))
                        .collect(Collectors.toList());
    }

    @Override
    public void execute(
            ChangeEventSourceContext context, Oracle9BridgeOffsetContext offsetContext) {
        this.context = context;
        try {
            log.info(
                    "Start {} from fzsFileNumber={}, scn={}",
                    getClass().getName(),
                    offsetContext.getFzsFileNumber(),
                    offsetContext.getScn());
            long pollInterval = sourceConfig.getDbzConnectorConfig().getPollInterval().toMillis();
            OracleAgentClient oracle9BridgeClient =
                    OracleAgentClientFactory.getOrCreateStartedSocketClient(
                            sourceConfig.getOracle9BridgeHost(),
                            sourceConfig.getOracle9BridgePort());
            Integer currentFzsFileNumber = offsetContext.getFzsFileNumber();
            if (offsetContext.getFzsFileNumber() == 0) {
                currentFzsFileNumber =
                        Oracle9BridgeClientUtils.currentMinFzsFileNumber(
                                oracle9BridgeClient,
                                new OracleTransactionFileNumberFetchRequest(tables, tableOwners));
            }

            while (context.isRunning()) {
                List<OracleTransactionData> oracleTransactionData =
                        Oracle9BridgeClientUtils.fetchOracleTransactionData(
                                oracle9BridgeClient, tableOwners, tables, currentFzsFileNumber);
                if (CollectionUtils.isEmpty(oracleTransactionData)) {
                    log.debug(
                            "There is no data for tables: {} in the current fzs file: {}",
                            tables,
                            currentFzsFileNumber);
                    Integer maxFzsFileNumber =
                            Oracle9BridgeClientUtils.currentMaxFzsFileNumber(
                                    oracle9BridgeClient, tableOwners, tables);
                    if (currentFzsFileNumber < maxFzsFileNumber) {
                        log.info("The fzs file: {} is broken will skip it", currentFzsFileNumber);
                        currentFzsFileNumber++;
                    } else {
                        log.info(
                                "There is no data in the related fzs files: {}",
                                currentFzsFileNumber);
                        Thread.sleep(NO_DATA_AVAILABLE_SLEEP_MS);
                    }
                    continue;
                }
                for (OracleTransactionData data : oracleTransactionData) {
                    // todo: filter the already processed scn in snapshot stage
                    handleEvent(offsetContext, currentFzsFileNumber, data.getOp());
                }
                Scn preScn = offsetContext.getScn();
                if (offsetContext.getScn().compareTo(preScn) != 1) {
                    eventDispatcher.dispatchHeartbeatEvent(offsetContext);
                }
                currentFzsFileNumber++;
                Thread.sleep(pollInterval);
            }
        } catch (Exception e) {
            log.error("Fzs fetch task stopped due to the {}", e.getMessage(), e);
            errorHandler.setProducerThrowable(e);
        }
    }

    protected void handleEvent(
            Oracle9BridgeOffsetContext offsetContext,
            Integer fzsFileNumber,
            List<OracleOperation> oracleOperations) {
        for (OracleOperation oracleOperation : oracleOperations) {
            TableId tableId = tableNameToIdMap.get(oracleOperation.getTable());
            Table table = oracleDatabaseSchema.tableFor(tableId);
            Scn scn = new Scn(new BigInteger(oracleOperation.getScn(), 16));
            if (OracleDDLOperation.TYPE.equals(oracleOperation.getType())) {
                log.info(
                        "The DDL: {} of the oracle 9 bridge cdc connector is not supported, will skip it",
                        oracleOperation);
                offsetContext.event(tableId, clock.currentTime());
                offsetContext.setScn(scn);
                offsetContext.setFzsFileNumber(fzsFileNumber);
                continue;
            }
            List<Oracle9BridgeDmlEntry> dmlEntries =
                    Oracle9BridgeDmlEntryFactory.transformOperation(
                            oracleValueConverters, oracleOperation, table);
            for (Oracle9BridgeDmlEntry dmlEntry : dmlEntries) {
                offsetContext.event(tableId, clock.currentTime());
                offsetContext.setScn(scn);
                offsetContext.setFzsFileNumber(fzsFileNumber);
                try {
                    eventDispatcher.dispatchDataChangeEvent(
                            tableId,
                            new OracleDataChangeRecordEmitter(offsetContext, clock, dmlEntry));
                } catch (InterruptedException e) {
                    throw new RuntimeException("Dispatch DataChange Event Error", e);
                }
            }
        }
    }
}
