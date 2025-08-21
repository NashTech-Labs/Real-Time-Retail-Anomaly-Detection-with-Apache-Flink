package com.nashtech.demo.flink.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Represents only refund-type POS events (derived from PosEvent).
 * Use case: Drives suspicious refund detection logic.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RefundEvent {
    private String refundId;
    private String transactionId;
    private String storeId;
    private String cashierId;
    private double amount;
    private long eventTime;

    @Override
    public String toString() {
        return String.format("RefundEvent[id=%s txn=%s store=%s cashier=%s amount=%.2f time=%s]",
                refundId, transactionId, storeId, cashierId, amount, Instant.ofEpochMilli(eventTime));
    }
}
