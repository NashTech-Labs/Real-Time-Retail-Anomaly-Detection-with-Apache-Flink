package com.nashtech.demo.flink.functions;

import com.nashtech.demo.flink.model.AlertEvent;
import com.nashtech.demo.flink.model.EnrichedPosEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RefundPatternFunctionTest {

    private RefundPatternFunction function;

    private OneInputStreamOperatorTestHarness<EnrichedPosEvent, AlertEvent> harness;

    @BeforeEach
    void setup() throws Exception {
        function = new RefundPatternFunction(
                100.0,  // amountThreshold
                3,      // countThreshold
                600_000L, // windowMillis
                60_000L   // saleTtlMillis
        );

        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(function),
                EnrichedPosEvent::getStoreId,
                TypeInformation.of(String.class)
        );
        harness.open();
    }

    @Test
    void testRefundWithoutSaleGeneratesAlert() throws Exception {
        EnrichedPosEvent refund = EnrichedPosEvent.builder()
                .eventType("REFUND")
                .transactionId("txn-1")
                .storeId("store-001")
                .cashierId("cashier-1")
                .amount(150.0)
                .eventTime(Instant.now().toEpochMilli())
                .build();

        harness.processElement(refund, refund.getEventTime());

        List<AlertEvent> output = harness.extractOutputValues();
        assertThat(output).hasSize(1);
        assertThat(output.get(0).getMessage()).contains("refund_without_sale");
    }

    @Test
    void testSaleThenRefundDoesNotTriggerRefundWithoutSale() throws Exception {
        long now = Instant.now().toEpochMilli();

        EnrichedPosEvent sale = EnrichedPosEvent.builder()
                .eventType("SALE")
                .transactionId("txn-2")
                .storeId("store-001")
                .cashierId("cashier-1")
                .amount(200.0)
                .eventTime(now)
                .build();

        EnrichedPosEvent refund = EnrichedPosEvent.builder()
                .eventType("REFUND")
                .transactionId("txn-2")
                .storeId("store-001")
                .cashierId("cashier-1")
                .amount(200.0)
                .eventTime(now + 1)
                .build();

        harness.processElement(sale, now);
        harness.processElement(refund, now + 1);

        List<AlertEvent> output = harness.extractOutputValues();
        assertThat(output).isEmpty(); // no "refund_without_sale"
    }

    @Test
    void testRefundBurstGeneratesAlert() throws Exception {
        long base = Instant.now().toEpochMilli();

        for (int i = 0; i < 3; i++) {
            EnrichedPosEvent refund = EnrichedPosEvent.builder()
                    .eventType("REFUND")
                    .transactionId("txn-" + i)
                    .storeId("store-001")
                    .cashierId("cashier-1")
                    .amount(200.0) // above threshold
                    .eventTime(base + i)
                    .build();
            harness.processElement(refund, base + i);
        }

        List<AlertEvent> output = harness.extractOutputValues();
        assertThat(output).anyMatch(alert -> alert.getMessage().contains("refund_burst"));
    }

}
