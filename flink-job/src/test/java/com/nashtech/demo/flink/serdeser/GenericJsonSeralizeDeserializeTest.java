package com.nashtech.demo.flink.serdeser;

import com.nashtech.demo.flink.model.AlertEvent;
import com.nashtech.demo.flink.model.EnrichedPosEvent;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;

class GenericJsonSeralizeDeserializeDomainTest {

    @Test
    void testSerializeDeserializeAlertEvent() throws IOException {
        GenericJsonSeralizeDeserialize<AlertEvent> serde =
                new GenericJsonSeralizeDeserialize<>(AlertEvent.class);

        AlertEvent original = AlertEvent.builder()
                .alertId(UUID.randomUUID().toString())
                .employId("emp-101")
                .storeId("store-1")
                .type("SECURITY_CORRELATION")
                .message("Test alert")
                .riskScore(0.9)
                .alertTime(Instant.parse("2025-08-16T15:10:00Z"))
                .build();

        byte[] bytes = serde.serialize(original);
        AlertEvent restored = serde.deserialize(bytes);

        assertEquals(original.getAlertId(), restored.getAlertId());
        assertEquals(original.getEmployId(), restored.getEmployId());
        assertEquals(original.getStoreId(), restored.getStoreId());
        assertEquals(original.getType(), restored.getType());
        assertEquals(original.getMessage(), restored.getMessage());
        assertEquals(original.getRiskScore(), restored.getRiskScore());
        assertEquals(original.getAlertTime(), restored.getAlertTime());
    }

    @Test
    void testSerializeDeserializeEnrichedPosEvent() throws IOException {
        GenericJsonSeralizeDeserialize<EnrichedPosEvent> serde =
                new GenericJsonSeralizeDeserialize<>(EnrichedPosEvent.class);

        EnrichedPosEvent original = EnrichedPosEvent.builder()
                .transactionId("txn-555")
                .storeId("store-9")
                .cashierId("cashier-7")
                .eventType("SALE")
                .amount(123.45)
                .sku("sku-abc")
                .eventTime(Instant.now().toEpochMilli())
                .watchlistFlag(false)
                .build();

        byte[] bytes = serde.serialize(original);
        EnrichedPosEvent restored = serde.deserialize(bytes);

        assertEquals(original.getTransactionId(), restored.getTransactionId());
        assertEquals(original.getStoreId(), restored.getStoreId());
        assertEquals(original.getCashierId(), restored.getCashierId());
        assertEquals(original.getEventType(), restored.getEventType());
        assertEquals(original.getAmount(), restored.getAmount());
        assertEquals(original.getSku(), restored.getSku());
        assertEquals(original.getEventTime(), restored.getEventTime());
        assertEquals(original.isWatchlistFlag(), restored.isWatchlistFlag());
    }

    @Test
    void testIsEndOfStream() {
        GenericJsonSeralizeDeserialize<AlertEvent> serde =
                new GenericJsonSeralizeDeserialize<>(AlertEvent.class);

        AlertEvent alert = AlertEvent.builder()
                .alertId("id")
                .storeId("s")
                .employId("e")
                .type("t")
                .message("m")
                .alertTime(Instant.now())
                .riskScore(0.5)
                .build();

        assertFalse(serde.isEndOfStream(alert));
    }

    @Test
    void testProducedTypeForAlertEvent() {
        GenericJsonSeralizeDeserialize<AlertEvent> serde =
                new GenericJsonSeralizeDeserialize<>(AlertEvent.class);

        assertEquals(AlertEvent.class, serde.getProducedType().getTypeClass());
    }

    @Test
    void testProducedTypeForEnrichedPosEvent() {
        GenericJsonSeralizeDeserialize<EnrichedPosEvent> serde =
                new GenericJsonSeralizeDeserialize<>(EnrichedPosEvent.class);

        assertEquals(EnrichedPosEvent.class, serde.getProducedType().getTypeClass());
    }
}

