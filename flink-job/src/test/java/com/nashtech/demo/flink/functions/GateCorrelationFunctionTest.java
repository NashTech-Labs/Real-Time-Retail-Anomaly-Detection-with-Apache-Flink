package com.nashtech.demo.flink.functions;

import com.nashtech.demo.flink.model.AlertEvent;
import com.nashtech.demo.flink.model.EnrichedPosEvent;
import com.nashtech.demo.flink.model.SecurityEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class GateCorrelationFunctionTest {

    private KeyedTwoInputStreamOperatorTestHarness<String, EnrichedPosEvent, SecurityEvent, AlertEvent> harness;

    @BeforeEach
    void setup() throws Exception {
        GateCorrelationFunction function = new GateCorrelationFunction(5000L, 200.0);
        harness = new KeyedTwoInputStreamOperatorTestHarness<>(
                new KeyedCoProcessOperator<>(function),
                EnrichedPosEvent::getStoreId, // keySelector for POS events
                SecurityEvent::getStoreId,   // keySelector for security events
                TypeInformation.of(String.class)
        );
        harness.open();
    }

    @AfterEach
    void cleanup() throws Exception {
        harness.close();
    }

    @Test
    void testNoAlertForLowValueSale() throws Exception {
        EnrichedPosEvent sale = TestData.sale("txn1", "store1", "cash1", 50.0, 1000L);
        harness.processElement1(sale, 1000L);

        SecurityEvent gate = TestData.security("store1", 1500L, "gateA");
        harness.processElement2(gate, 1500L);

        List<AlertEvent> output = HarnessUtils.getSecurityOutput(harness);
        assertTrue(output.isEmpty(), "No alert should be emitted for low-value sale");
    }

    @Test
    void testAlertOnHighValueSaleAndGate() throws Exception {
        EnrichedPosEvent sale = TestData.sale("txn2", "store1", "cash1", 500.0, 2000L);
        harness.processElement1(sale, 2000L);

        SecurityEvent gate = TestData.security("store1", 2500L, "gateB");
        harness.processElement2(gate, 2500L);

        List<AlertEvent> output = HarnessUtils.getSecurityOutput(harness);
        assertEquals(1, output.size(), "One alert should be emitted");
        AlertEvent alert = output.get(0);
        assertEquals("SECURITY_CORRELATION", alert.getType());
        assertEquals("store1", alert.getStoreId());
        assertEquals("cash1", alert.getEmployId());
        assertTrue(alert.getMessage().contains("gateB"));
    }

    @Test
    void testNoAlertWhenOutsideWindow() throws Exception {
        EnrichedPosEvent sale = TestData.sale("txn3", "store1", "cash2", 600.0, 1000L);
        harness.processElement1(sale, 1000L);

        // Security event comes too late
        SecurityEvent gate = TestData.security("store1", 7000L, "gateC");
        harness.processElement2(gate, 7000L);

        List<AlertEvent> output = HarnessUtils.getSecurityOutput(harness);
        assertTrue(output.isEmpty(), "No alert should be emitted when outside correlation window");
    }
}
