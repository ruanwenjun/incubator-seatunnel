package io.debezium.connector.oracle;

import io.debezium.config.Configuration;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;

@SuppressWarnings("MagicNumber")
public class OracleAgentConnectorConfig extends OracleConnectorConfig {

    public OracleAgentConnectorConfig(Configuration config) {
        super(config);
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(
            Version version) {
        return new OracleAgentSourceInfoStructMaker(Module.name(), Module.version(), this);
    }
}
