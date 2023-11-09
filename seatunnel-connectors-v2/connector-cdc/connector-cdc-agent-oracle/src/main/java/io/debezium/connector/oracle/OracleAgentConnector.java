package io.debezium.connector.oracle;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;

import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;

import java.util.List;
import java.util.Map;

public class OracleAgentConnector extends RelationalBaseSourceConnector {

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(OracleAgentConnectorConfig.ALL_FIELDS);
    }

    @Override
    public void start(Map<String, String> map) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<? extends Task> taskClass() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void stop() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConfigDef config() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String version() {
        return Module.version();
    }
}
