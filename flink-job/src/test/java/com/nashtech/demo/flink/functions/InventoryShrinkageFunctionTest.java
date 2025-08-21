package com.nashtech.demo.flink.functions;

import static org.junit.jupiter.api.Assertions.*;

import com.nashtech.demo.flink.model.AlertEvent;
import com.nashtech.demo.flink.model.EnrichedPosEvent;
import com.nashtech.demo.flink.model.InventoryCount;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.*;

import java.util.List;

class InventoryShrinkageFunctionTest {

    private KeyedTwoInputStreamOperatorTestHarness<String, EnrichedPosEvent, InventoryCount, AlertEvent> harness;

    @BeforeEach
    void setup() throws Exception {
        InventoryShrinkageFunction function = new InventoryShrinkageFunction(5L);
        harness = new KeyedTwoInputStreamOperatorTestHarness<>(
                new KeyedCoProcessOperator<>(function),
                e -> e.getStoreId() + ":" + e.getSku(),   // keySelector for sales
                inv -> inv.getStoreId() + ":" + inv.getSku(), // keySelector for inventory
                TypeInformation.of(String.class)
        );
        harness.open();
    }

    @AfterEach
    void cleanup() throws Exception {
        harness.close();
    }

    @Test
    void testNoAlertWhenDiffBelowThreshold() throws Exception {
        // 2 sales
        harness.processElement1(TestData.sale("txn1", "store1", "cash1", "skuA", 100.0, 1000L), 1000L);
        harness.processElement1(TestData.sale("txn2", "store1", "cash1", "skuA", 200.0, 2005L), 2005L);

        // inventory shows 1 item -> diff = 2 - 1 = 1 < threshold(5)
        harness.processElement2(TestData.inventory("store1", "skuA", "mgr1", 1, 3000L), 3000L);

        List<AlertEvent> output = HarnessUtils.getInventoryShrinkageOutput(harness);
        assertTrue(output.isEmpty(), "No alert should be emitted when diff < threshold");
    }

    @Test
    void testAlertWhenDiffExceedsThreshold() throws Exception {
        // simulate 7 sales
        for (int i = 0; i < 7; i++) {
            harness.processElement1(TestData.sale("txn" + i, "store1", "cash1", "skuB", 50.0, 1000L + i), 1000L + i);
        }

        // inventory says 1 item counted -> diff = 7 - 1 = 6 >= threshold(5)
        harness.processElement2(TestData.inventory("store1", "skuB", "mgr1", 1, 4000L), 4000L);

        List<AlertEvent> output = HarnessUtils.getInventoryShrinkageOutput(harness);
        assertEquals(1, output.size(), "One alert should be emitted");
        AlertEvent alert = output.get(0);
        assertEquals("SHRINKAGE", alert.getType());
        assertEquals("store1", alert.getStoreId());
        assertTrue(alert.getMessage().contains("sku=skuB"));
        assertTrue(alert.getMessage().contains("diff=6"));
        assertTrue(alert.getRiskScore() > 0.9, "RiskScore should be normalized to <=1.0");
    }

    @Test
    void testCounterResetsAfterInventory() throws Exception {
        // 5 sales
        for (int i = 0; i < 5; i++) {
            harness.processElement1(TestData.sale("txn" + i, "store2", "cash2", "skuC", 80.0, 2000L + i), 2000L + i);
        }

        // first inventory count triggers an alert (diff=5-0=5, threshold=5)
        harness.processElement2(TestData.inventory("store2", "skuC", "mgr2", 0, 3000L), 3000L);
        List<AlertEvent> firstOutput = HarnessUtils.getInventoryShrinkageOutput(harness);
        assertEquals(1, firstOutput.size(), "First snapshot should trigger alert");

        // after reset, another snapshot with no new sales → diff=0 → no alert
        harness.processElement2(TestData.inventory("store2", "skuC", "mgr2", 0, 5000L), 5000L);
        List<AlertEvent> secondOutput = HarnessUtils.getInventoryShrinkageOutput(harness);
        assertEquals(1, secondOutput.size(), "No new alert should be emitted after reset");
    }
}
