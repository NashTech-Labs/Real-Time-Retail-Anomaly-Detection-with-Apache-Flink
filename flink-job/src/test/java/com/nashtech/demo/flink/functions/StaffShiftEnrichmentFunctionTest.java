package com.nashtech.demo.flink.functions;

import com.nashtech.demo.flink.model.EnrichedPosEvent;
import com.nashtech.demo.flink.model.PosEvent;
import com.nashtech.demo.flink.model.StaffShift;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class StaffShiftEnrichmentFunctionTest {

    private KeyedBroadcastOperatorTestHarness<String, PosEvent, StaffShift, EnrichedPosEvent> harness;

    @BeforeEach
    void setup() throws Exception {
        StaffShiftEnrichmentFunction function = new StaffShiftEnrichmentFunction();

        harness = new KeyedBroadcastOperatorTestHarness<>(
                new CoBroadcastWithKeyedOperator<>(function,
                        List.of(StaffShiftEnrichmentFunction.BROADCAST_STATE_DESC)),
                PosEvent::getCashierId, // key selector
                TypeInformation.of(String.class),
                2, 1, 0
        );
        harness.open();
    }

    @AfterEach
    void cleanup() throws Exception {
        harness.close();
    }

    @Test
    void testBroadcastThenEventEnrichment() throws Exception {
        // broadcast a shift
        StaffShift shift = TestData.shift("c1", "store1", 1000L, 5000L);
        harness.processBroadcastElement(
                shift,
                100L
        );

        // process a POS event
        PosEvent pos = TestData.pos("txn1", "store1", "c1", "SALE", 25.0, "skuX", 2000L);
        harness.processElement(pos, 2000L);

        List<EnrichedPosEvent> output = HarnessUtils.getEnrichedPos(harness);
        Assertions.assertEquals(1, output.size());
        EnrichedPosEvent enriched = output.get(0);

        Assertions.assertEquals("c1", enriched.getCashierId());
        assertNotNull(enriched.getShiftInfo());
        Assertions.assertEquals("store1", enriched.getShiftInfo().getStoreId());
    }

    @Test
    void testEventWithoutShift() throws Exception {
        PosEvent pos = TestData.pos("txn2", "store2", "c2", "SALE", 50.0, "skuY", 3000L);
        harness.processElement(pos, 3000L);

        List<EnrichedPosEvent> output = HarnessUtils.getEnrichedPos(harness);
        Assertions.assertEquals(1, output.size());
        EnrichedPosEvent enriched = output.get(0);

        Assertions.assertEquals("c2", enriched.getCashierId());
        assertNull(enriched.getShiftInfo());
    }
}