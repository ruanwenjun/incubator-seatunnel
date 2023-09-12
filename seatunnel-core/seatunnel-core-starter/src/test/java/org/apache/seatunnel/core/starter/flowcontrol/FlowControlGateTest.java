package org.apache.seatunnel.core.starter.flowcontrol;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlowControlGateTest {

    private static final int rowSize = 181;

    @Test
    public void testWithBytes() {
        Clock clock = Clock.systemDefaultZone();
        FlowControlGate flowControlGate = FlowControlGate.create(FlowControlStrategy.ofBytes(100));
        List<SeaTunnelRow> rows = getRows(10);
        long start = clock.millis();
        for (SeaTunnelRow row : rows) {
            flowControlGate.audit(row);
        }
        long end = clock.millis();
        long useTime = rowSize * 10 / 100 * 1000;

        Assertions.assertTrue(end - start > useTime * 0.8 && end - start < useTime * 1.2);
    }

    @Test
    public void testWithCount() {
        Clock clock = Clock.systemDefaultZone();
        FlowControlGate flowControlGate = FlowControlGate.create(FlowControlStrategy.ofCount(2));
        List<SeaTunnelRow> rows = getRows(10);
        long start = clock.millis();
        for (SeaTunnelRow row : rows) {
            flowControlGate.audit(row);
        }
        long end = clock.millis();
        long useTime = 10 / 2 * 1000;

        Assertions.assertTrue(end - start > useTime * 0.8 && end - start < useTime * 1.2);
    }

    @Test
    public void testWithBytesAndCount() {
        Clock clock = Clock.systemDefaultZone();
        FlowControlGate flowControlGate = FlowControlGate.create(FlowControlStrategy.of(100, 2));
        List<SeaTunnelRow> rows = getRows(10);
        long start = clock.millis();
        for (SeaTunnelRow row : rows) {
            flowControlGate.audit(row);
        }
        long end = clock.millis();
        long useTime = rowSize * 10 / 100 * 1000;

        Assertions.assertTrue(end - start > useTime * 0.8 && end - start < useTime * 1.2);
    }

    /** return row list with size, each row size is 181 */
    private List<SeaTunnelRow> getRows(int size) {
        Map<String, Object> map = new HashMap<>();
        map.put(
                "key1",
                new SeaTunnelRow(
                        new Object[] {
                            1, "test", 1L, new BigDecimal("3333.333"),
                        }));
        map.put(
                "key2",
                new SeaTunnelRow(
                        new Object[] {
                            1, "test", 1L, new BigDecimal("3333.333"),
                        }));

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            rows.add(
                    new SeaTunnelRow(
                            new Object[] {
                                1,
                                "test",
                                1L,
                                map,
                                new BigDecimal("3333.333"),
                                new String[] {"test2", "test", "3333.333"}
                            }));
        }
        return rows;
    }
}
