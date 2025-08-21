package com.nashtech.demo.flink.functions;

import com.nashtech.demo.flink.model.AlertEvent;
import com.nashtech.demo.flink.model.EnrichedPosEvent;
import com.nashtech.demo.flink.model.InventoryCount;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.UUID;

/**
 * InventoryShrinkageFunction detects potential shrinkage (loss of inventory not explained by sales)
 * by correlating point-of-sale events with periodic physical inventory counts.
 *
 * <p>This function consumes two keyed streams:
 * <ul>
 *   <li>{@link EnrichedPosEvent} (point-of-sale enriched events)</li>
 *   <li>{@link InventoryCount} (physical or system-recorded stock counts)</li>
 * </ul>
 *
 * <p>The key is assumed to be {@code storeId:sku} to allow correlation at store and product level.
 *
 * <p>Logic:
 * <ul>
 *   <li>Counts the number of SALE events since the last inventory snapshot.</li>
 *   <li>On receiving an {@link InventoryCount}, compares the sales count against the
 *       reported stock quantity.</li>
 *   <li>If the difference (sales - inventoryCountedQty) exceeds {@code diffThreshold},
 *       an {@link AlertEvent} is generated to flag potential shrinkage.</li>
 * </ul>
 *
 * <p>State:
 * <ul>
 *   <li>{@code salesSinceSnapshot} - running count of sales since the last inventory snapshot.</li>
 * </ul>
 *
 * <p>After processing an inventory snapshot, the sales counter is reset.
 *
 * <p>Typical use case: Detecting theft, fraud, or miscounts when sales data does not reconcile
 * with physical stock levels.
 */
@Slf4j
public class InventoryShrinkageFunction extends
        KeyedCoProcessFunction<String, EnrichedPosEvent, InventoryCount, AlertEvent> {

    /** Difference threshold beyond which shrinkage is considered suspicious. */
    private final long diffThreshold;

    /** State: number of sales recorded since the last inventory count snapshot. */
    private transient ValueState<Long> salesSinceSnapshot;

    /**
     * Creates a new InventoryShrinkageFunction.
     *
     * @param diffThreshold the minimum difference between sales and inventory that triggers an alert
     */
    public InventoryShrinkageFunction(long diffThreshold) {
        this.diffThreshold = diffThreshold;
    }

    /**
     * Initializes Flink managed state for tracking sales since last inventory snapshot.
     */
    @Override
    public void open(OpenContext context) throws Exception {
        salesSinceSnapshot = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("salesSinceSnapshot", Long.class));
    }

    /**
     * Processes {@link EnrichedPosEvent} events.
     * <p>If the event type is "SALE", increments the sales counter in state.</p>
     *
     * @param enriched POS event (only SALE events are counted)
     * @param ctx context with time and state access
     * @param out collector for emitting alerts (not used here)
     */
    @Override
    public void processElement1(EnrichedPosEvent enriched, Context ctx, Collector<AlertEvent> out) throws Exception {
        if (enriched == null || !"SALE".equalsIgnoreCase(enriched.getEventType())) return;

        Long current = salesSinceSnapshot.value();
        current = (current == null) ? 0L : current;
        salesSinceSnapshot.update(current + 1L);
    }

    /**
     * Processes {@link InventoryCount} events.
     * <p>Compares accumulated sales with the latest inventory snapshot.</p>
     *
     * <ul>
     *   <li>If difference ≥ {@code diffThreshold}, emits a {@link AlertEvent} of type SHRINKAGE.</li>
     *   <li>After processing, resets sales counter.</li>
     * </ul>
     *
     * @param count latest inventory snapshot
     * @param ctx context with time and state access
     * @param out collector for emitting alerts
     */
    @Override
    public void processElement2(InventoryCount count, Context ctx, Collector<AlertEvent> out) throws Exception {
        if (count == null) return;
        Long sales = salesSinceSnapshot.value();
        sales = (sales == null) ? 0L : sales;

        long diff = sales - count.getCountedQty();
        if (diff >= diffThreshold) {
            String key = ctx.getCurrentKey();
            String[] parts = key != null ? key.split(":", 2) : new String[]{null, null};
            String storeId = parts.length > 0 ? parts[0] : null;
            String sku = parts.length > 1 ? parts[1] : null;

            AlertEvent alert = AlertEvent.builder()
                    .employId(count.getManagerId())
                    .alertId(UUID.randomUUID().toString())
                    .storeId(storeId)
                    .type("SHRINKAGE")
                    .message(String.format("shrinkage sku=%s sales=%d inventoryCounted=%d diff=%d",
                            sku, sales, count.getCountedQty(), diff))
                    .riskScore(Math.min(1.0, diff / (double) diffThreshold))
                    .alertTime(Instant.now())
                    .build();
            out.collect(alert);
            log.info("Shrinkage alert store={} sku={} diff={}", storeId, sku, diff);
        }

        // reset sales counter after snapshot
        salesSinceSnapshot.clear();
    }
}
