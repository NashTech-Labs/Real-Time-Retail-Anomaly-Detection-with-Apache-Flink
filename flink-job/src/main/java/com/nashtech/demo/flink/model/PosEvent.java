package com.nashtech.demo.flink.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;


/**
 * Represents a single POS (Point-of-Sale) transaction event.
 * Use case: Base event for all retail transactions (sales, refunds, voids, etc.).
 *           Will be enriched with staff shift data before going into detection logic.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PosEvent {
    private String transactionId;
    private String storeId;
    private String cashierId;
    private String sku;
    private double amount;
    private String eventType;
    private long eventTime;

    @Override
    public String toString() {
        return String.format("PosEvent[txn=%s store=%s cashier=%s sku=%s amount=%.2f type=%s time=%s]",
                transactionId, storeId, cashierId, sku, amount, eventType, Instant.ofEpochMilli(eventTime));
    }
}