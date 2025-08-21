package com.nashtech.demo.flink.model;

import lombok.*;

import java.time.Instant;

/**
 * Represents periodic inventory count data from stocktaking systems.
 * Use case: Allows shrinkage detection by comparing actual counts to expected levels from sales data.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryCount {
    private String storeId;
    private String managerId;
    private String sku;
    private long countedQty;
    private long eventTime;

    @Override
    public String toString() {
        return String.format("InventoryCount[store=%s sku=%s qty=%d time=%s]",
                storeId, sku, countedQty, Instant.ofEpochMilli(eventTime));
    }
}