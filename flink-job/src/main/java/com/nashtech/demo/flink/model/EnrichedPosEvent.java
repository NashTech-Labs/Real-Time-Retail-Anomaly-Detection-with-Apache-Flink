package com.nashtech.demo.flink.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents an enriched POS event that includes shift and other contextual information.
 * Use case: Provides full context to downstream detection logic (patterns, ML scoring, etc.).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EnrichedPosEvent {
    private String transactionId;
    private String storeId;
    private String cashierId;
    private String eventType;
    private String sku;
    private double amount;
    private long eventTime;
    private StaffShift shiftInfo;
    private boolean watchlistFlag;
}
