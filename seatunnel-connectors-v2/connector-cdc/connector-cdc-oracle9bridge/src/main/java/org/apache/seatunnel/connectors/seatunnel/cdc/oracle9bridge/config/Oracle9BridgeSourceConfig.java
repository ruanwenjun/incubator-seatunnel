package org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.config;

import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;

import io.debezium.connector.oracle.Oracle9BridgeConnectorConfig;
import io.debezium.relational.RelationalTableFilters;
import lombok.Getter;

import java.util.List;
import java.util.Properties;

public class Oracle9BridgeSourceConfig extends JdbcSourceConfig {

    private static final long serialVersionUID = 1L;

    @Getter private final String oracle9BridgeHost;
    @Getter private final Integer oracle9BridgePort;

    public Oracle9BridgeSourceConfig(
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
        this.oracle9BridgeHost = oracle9BridgeHost;
        this.oracle9BridgePort = oracle9BridgePort;
    }

    @Override
    public Oracle9BridgeConnectorConfig getDbzConnectorConfig() {
        return new Oracle9BridgeConnectorConfig(getDbzConfiguration());
    }

    public RelationalTableFilters getTableFilters() {
        return getDbzConnectorConfig().getTableFilters();
    }
}
