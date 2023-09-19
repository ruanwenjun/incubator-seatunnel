package org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.source;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class Oracle9BridgeIncrementalSourceFactoryTest {

    @Test
    void optionRule() {
        Assertions.assertNotNull((new Oracle9BridgeIncrementalSourceFactory()).optionRule());
    }
}
