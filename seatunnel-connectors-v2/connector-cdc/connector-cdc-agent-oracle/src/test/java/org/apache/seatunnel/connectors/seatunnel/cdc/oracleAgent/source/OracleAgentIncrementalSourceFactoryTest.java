package org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OracleAgentIncrementalSourceFactoryTest {

    @Test
    void optionRule() {
        Assertions.assertNotNull((new OracleAgentIncrementalSourceFactory()).optionRule());
    }
}
