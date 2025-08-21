package com.nashtech.demo.flink.functions;

import com.nashtech.demo.flink.model.AlertEvent;
import com.nashtech.demo.flink.model.EnrichedPosEvent;
import com.nashtech.demo.flink.model.PosEvent;
import com.nashtech.demo.flink.model.SecurityEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.UUID;

/**
 * GateCorrelationFunction detects potential theft or suspicious activity by correlating
 * high-value sales with exit gate trigger events within a specified time window.
 *
 * <p>This function consumes two keyed streams:
 * <ul>
 *   <li>{@link EnrichedPosEvent} (point-of-sale enriched events)</li>
 *   <li>{@link SecurityEvent} (security system gate triggers)</li>
 * </ul>
 *
 * <p>If a high-value sale (above {@code highValueThreshold}) is followed by a gate trigger
 * within {@code correlationWindowMillis}, an {@link AlertEvent} is generated.
 *
 * <p>Typical use case: Detecting cases where items are purchased (or faked in transactions)
 * and suspiciously carried through an exit gate shortly after the sale.
 *
 * <p>Data flow:
 * <pre>
 *   EnrichedPosEvent (SALE >= threshold) --> store state
 *   SecurityEvent (exit trigger)         --> check correlation
 *   Match (within window)                --> emit AlertEvent
 * </pre>
 *
 * State:
 * <ul>
 *   <li>{@code lastHighSale} - Stores the most recent high-value sale per key.</li>
 * </ul>
 *
 * Timers:
 * <ul>
 *   <li>A timer clears {@code lastHighSale} after {@code correlationWindowMillis} expires
 *   to prevent stale correlations.</li>
 * </ul>
 */
@Slf4j
public class GateCorrelationFunction extends
        KeyedCoProcessFunction<String, EnrichedPosEvent, SecurityEvent, AlertEvent> {

    /** Time window (in milliseconds) during which a gate event may be correlated with a sale. */
    private final long correlationWindowMillis;

    /** Threshold amount above which a sale is considered "high-value". */
    private final double highValueThreshold;

    /** Stores the last high-value sale per key (store or cashier). */
    private transient ValueState<EnrichedPosEvent> lastHighSale;

    /**
     * Creates a GateCorrelationFunction.
     *
     * @param correlationWindowMillis window duration (ms) to check sale-gate correlation
     * @param highValueThreshold minimum sale amount to qualify as a high-value transaction
     */
    public GateCorrelationFunction(long correlationWindowMillis, double highValueThreshold) {
        this.correlationWindowMillis = correlationWindowMillis;
        this.highValueThreshold = highValueThreshold;
    }

    /**
     * Initializes state for storing high-value sales.
     */
    @Override
    public void open(OpenContext context) throws Exception {
        lastHighSale = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("lastHighSale", EnrichedPosEvent.class));
    }

    /**
     * Processes {@link EnrichedPosEvent}.
     * <p>If the event is a sale above the {@code highValueThreshold}, it is stored in state.
     * A timer is registered to clear the state after the correlation window expires.
     *
     * @param enriched POS event (sale or other)
     * @param ctx context with time and state access
     * @param out collector for emitting alerts
     */
    @Override
    public void processElement1(EnrichedPosEvent enriched, Context ctx, Collector<AlertEvent> out) throws Exception {
        if (enriched == null) return;

        if (enriched.getAmount() >= highValueThreshold) {
            lastHighSale.update(enriched);
            ctx.timerService().registerEventTimeTimer(enriched.getEventTime() + correlationWindowMillis + 1L);
            log.debug("Stored high sale txn={} amount={} for store={}",
                    enriched.getTransactionId(), enriched.getAmount(), enriched.getStoreId());
        }
    }

    /**
     * Processes {@link SecurityEvent}.
     * <p>If a gate trigger occurs within {@code correlationWindowMillis} of a high-value sale,
     * an {@link AlertEvent} is emitted.
     *
     * @param security security gate event
     * @param ctx context with time and state access
     * @param out collector for emitting alerts
     */
    @Override
    public void processElement2(SecurityEvent security, Context ctx, Collector<AlertEvent> out) throws Exception {
        if (security == null) return;
        EnrichedPosEvent sale = lastHighSale.value();
        if (sale == null) return;

        long age = Math.abs(security.getEventTime() - sale.getEventTime());
        if (age <= correlationWindowMillis) {
            AlertEvent alert = AlertEvent.builder()
                    .employId(sale.getCashierId())
                    .alertId(UUID.randomUUID().toString())
                    .storeId(security.getStoreId())
                    .message(String.format("gate_correlation saleTxn=%s gateId=%s",
                            sale.getTransactionId(), security.getEventData().getGateId()))
                    .riskScore(0.85)
                    .type("SECURITY_CORRELATION")
                    .alertTime(Instant.now())
                    .build();
            out.collect(alert);
            log.info("Gate correlation alert for store={}, saleTxn={}, gateId={}",
                    security.getStoreId(), sale.getTransactionId(), security.getEventData().getGateId());
            lastHighSale.clear();
        }
    }

    /**
     * Timer callback to clear expired state.
     *
     * @param timestamp timer timestamp
     * @param ctx timer context
     * @param out collector for emitting alerts
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AlertEvent> out) {
        lastHighSale.clear();
    }
}
