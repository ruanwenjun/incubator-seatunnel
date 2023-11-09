package org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.cdc.debezium.row.DebeziumJsonDeserializeSchema;
import org.apache.seatunnel.connectors.cdc.debezium.row.SeaTunnelRowDebeziumDeserializeSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.config.OracleAgentSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.config.OracleAgentSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.config.OracleAgentSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source.offset.OracleAgentOffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.utils.OracleConnectionUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.utils.OracleTypeUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;

import org.apache.kafka.connect.data.Struct;

import com.google.auto.service.AutoService;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.ConnectTableChangeSerializer;
import io.debezium.relational.history.TableChanges;
import lombok.NoArgsConstructor;

import java.sql.SQLException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.cdc.base.option.SourceOptions.DEBEZIUM_PROPERTIES;
import static org.apache.seatunnel.connectors.cdc.base.option.SourceOptions.FORMAT;
import static org.apache.seatunnel.connectors.cdc.debezium.DeserializeFormat.COMPATIBLE_DEBEZIUM_JSON;

@NoArgsConstructor
@AutoService(SeaTunnelSource.class)
public class OracleAgentIncrementalSource<T> extends IncrementalSource<T, JdbcSourceConfig>
        implements SupportParallelism {

    static final String IDENTIFIER = "OracleAgent-CDC";

    private OracleAgentSourceConfig sourceConfig;

    public OracleAgentIncrementalSource(
            ReadonlyConfig options,
            SeaTunnelDataType<SeaTunnelRow> dataType,
            List<CatalogTable> catalogTables) {
        super(options, dataType, catalogTables);
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public Option<StartupMode> getStartupModeOption() {
        return OracleAgentSourceOptions.STARTUP_MODE;
    }

    @Override
    public Option<StopMode> getStopModeOption() {
        return OracleAgentSourceOptions.STOP_MODE;
    }

    @Override
    public SourceConfig.Factory<JdbcSourceConfig> createSourceConfigFactory(ReadonlyConfig config) {
        OracleAgentSourceConfigFactory configFactory =
                new OracleAgentSourceConfigFactory(
                        config.get(OracleAgentSourceOptions.ORACLE9BRIDGE_AGENT_HOST),
                        config.get(OracleAgentSourceOptions.ORACLE9BRIDGE_AGENT_PORT));
        configFactory.fromReadonlyConfig(readonlyConfig);
        configFactory.startupOptions(startupConfig);
        configFactory.stopOptions(stopConfig);
        configFactory.originUrl(config.get(JdbcCatalogOptions.BASE_URL));
        return configFactory;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DebeziumDeserializationSchema<T> createDebeziumDeserializationSchema(
            ReadonlyConfig config) {
        Map<TableId, Struct> tableIdStructMap = tableChanges();
        if (COMPATIBLE_DEBEZIUM_JSON.equals(config.get(FORMAT))) {
            return (DebeziumDeserializationSchema<T>)
                    new DebeziumJsonDeserializeSchema(
                            config.get(DEBEZIUM_PROPERTIES), tableIdStructMap);
        }

        // TODO: support multi-table
        SeaTunnelDataType<SeaTunnelRow> physicalRowType;
        if (dataType == null) {
            TableId tableId = dataSourceDialect.discoverDataCollections(getSourceConfig()).get(0);
            Table table;
            try (OracleConnection oracleConnection =
                    OracleConnectionUtils.createOracleConnection(
                            getSourceConfig().getDbzConfiguration())) {
                table =
                        ((OracleAgentDialect) dataSourceDialect)
                                .queryTableSchema(oracleConnection, tableId)
                                .getTable();
            } catch (SQLException e) {
                throw new SeaTunnelException(e);
            }
            physicalRowType = OracleTypeUtils.convertFromTable(table);
        } else {
            physicalRowType = dataType;
        }
        String zoneId = config.get(JdbcSourceOptions.SERVER_TIME_ZONE);
        return (DebeziumDeserializationSchema<T>)
                SeaTunnelRowDebeziumDeserializeSchema.builder()
                        .setPhysicalRowType(physicalRowType)
                        .setResultTypeInfo(physicalRowType)
                        .setServerTimeZone(ZoneId.of(zoneId))
                        .setTableIdTableChangeMap(tableIdStructMap)
                        .build();
    }

    @Override
    public DataSourceDialect<JdbcSourceConfig> createDataSourceDialect(ReadonlyConfig config) {
        return new OracleAgentDialect(getSourceConfig());
    }

    @Override
    public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
        return new OracleAgentOffsetFactory(
                getSourceConfig(), (OracleAgentDialect) dataSourceDialect);
    }

    private synchronized OracleAgentSourceConfig getSourceConfig() {
        if (sourceConfig != null) {
            return sourceConfig;
        }
        sourceConfig = (OracleAgentSourceConfig) configFactory.create(0);
        return sourceConfig;
    }

    private Map<TableId, Struct> tableChanges() {
        OracleAgentSourceConfig jdbcSourceConfig =
                (OracleAgentSourceConfig) configFactory.create(0);
        OracleAgentDialect dialect = new OracleAgentDialect(jdbcSourceConfig);
        List<TableId> discoverTables = dialect.discoverDataCollections(jdbcSourceConfig);
        ConnectTableChangeSerializer connectTableChangeSerializer =
                new ConnectTableChangeSerializer();
        try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(jdbcSourceConfig)) {
            return discoverTables.stream()
                    .collect(
                            Collectors.toMap(
                                    Function.identity(),
                                    (tableId) -> {
                                        TableChanges tableChanges = new TableChanges();
                                        tableChanges.create(
                                                dialect.queryTableSchema(jdbcConnection, tableId)
                                                        .getTable());
                                        return connectTableChangeSerializer
                                                .serialize(tableChanges)
                                                .get(0);
                                    }));
        } catch (Exception e) {
            throw new SeaTunnelException(e);
        }
    }
}
