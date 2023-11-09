package org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.config;

import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;

import io.debezium.connector.oracle.OracleAgentConnectorConfig;
import io.debezium.relational.RelationalTableFilters;
import lombok.Getter;

import java.util.List;
import java.util.Properties;

public class OracleAgentSourceConfig extends JdbcSourceConfig {

    private static final long serialVersionUID = 1L;

    @Getter private final String oracleAgentHost;
    @Getter private final Integer oracleAgentPort;

    public OracleAgentSourceConfig(
            StartupConfig startupConfig,
            StopConfig stopConfig,
            List<String> databaseList,
            List<String> tableList,
            int splitSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            int sampleShardingThreshold,
            int inverseSamplingRate,
            Properties dbzProperties,
            String driverClassName,
            String hostname,
            int port,
            String oracle9BridgeHost,
            Integer oracle9BridgePort,
            String username,
            String password,
            String originUrl,
            int fetchSize,
            String serverTimeZone,
            long connectTimeoutMillis,
            int connectMaxRetries,
            int connectionPoolSize,
            boolean exactlyOnce) {
        super(
                startupConfig,
                stopConfig,
                databaseList,
                tableList,
                splitSize,
                distributionFactorUpper,
                distributionFactorLower,
                sampleShardingThreshold,
                inverseSamplingRate,
                dbzProperties,
                driverClassName,
                hostname,
                port,
                username,
                password,
                originUrl,
                fetchSize,
                serverTimeZone,
                connectTimeoutMillis,
                connectMaxRetries,
                connectionPoolSize,
                exactlyOnce);
        this.oracleAgentHost = oracle9BridgeHost;
        this.oracleAgentPort = oracle9BridgePort;
    }

    @Override
    public OracleAgentConnectorConfig getDbzConnectorConfig() {
        return new OracleAgentConnectorConfig(getDbzConfiguration());
    }

    public RelationalTableFilters getTableFilters() {
        return getDbzConnectorConfig().getTableFilters();
    }
}
