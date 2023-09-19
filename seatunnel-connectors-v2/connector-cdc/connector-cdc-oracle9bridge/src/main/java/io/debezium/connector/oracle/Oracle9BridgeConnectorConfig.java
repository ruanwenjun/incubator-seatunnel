package io.debezium.connector.oracle;

import io.debezium.config.Configuration;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;

@SuppressWarnings("MagicNumber")
public class Oracle9BridgeConnectorConfig extends OracleConnectorConfig {

    public Oracle9BridgeConnectorConfig(Configuration config) {
        super(config);
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(
            Version version) {
        return new Oracle9BridgeSourceInfoStructMaker(Module.name(), Module.version(), this);
    }
}
