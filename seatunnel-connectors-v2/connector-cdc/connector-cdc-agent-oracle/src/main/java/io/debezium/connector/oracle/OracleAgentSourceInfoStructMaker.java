package io.debezium.connector.oracle;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;

public class OracleAgentSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private final Schema schema;

    public OracleAgentSourceInfoStructMaker(
            String connector, String version, CommonConnectorConfig connectorConfig) {
        super(connector, version, connectorConfig);
        schema =
                commonSchemaBuilder()
                        .name("io.debezium.connector.oracleAgent.Source")
                        .field(SourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                        .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                        .field(SourceInfo.COMMIT_SCN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                        .field(SourceInfo.SCN_KEY, Schema.STRING_SCHEMA)
                        .field(SourceInfo.FZS_FILE_NUMBER_KEY, Schema.OPTIONAL_INT32_SCHEMA)
                        .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        Struct ret =
                super.commonStruct(sourceInfo)
                        .put(SourceInfo.SCHEMA_NAME_KEY, sourceInfo.tableSchema())
                        .put(SourceInfo.TABLE_NAME_KEY, sourceInfo.table());

        if (sourceInfo.getFzsFileNumber() != null) {
            ret.put(SourceInfo.FZS_FILE_NUMBER_KEY, sourceInfo.getFzsFileNumber());
        }

        if (sourceInfo.getScn() != null) {
            ret.put(SourceInfo.SCN_KEY, sourceInfo.getScn().toString());
        }

        if (sourceInfo.getCommitScn() != null) {
            ret.put(SourceInfo.COMMIT_SCN_KEY, sourceInfo.getCommitScn().longValue());
        }
        return ret;
    }
}
