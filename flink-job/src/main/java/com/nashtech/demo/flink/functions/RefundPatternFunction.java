package com.nashtech.demo.flink.functions;

import com.nashtech.demo.flink.model.AlertEvent;
import com.nashtech.demo.flink.model.EnrichedPosEvent;
import com.nashtech.demo.flink.model.RefundEvent;
import com.nashtech.demo.flink.model.StaffShift;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.util.*;

/**
 * RefundPatternFunction detects suspicious refund patterns in POS transaction streams
 * and emits {@link AlertEvent} when anomalies are found.
 *
 * <p>This function consumes {@link EnrichedPosEvent} keyed by {@code storeId:cashierId} (or similar)
 * and analyzes refund behavior over a sliding time window.</p>
 *
 * <h3>Detection Logic:</h3>
 * <ul>
 *   <li><b>Refund Without Sale</b>: If a refund is observed without a matching prior SALE event,
 *       emit a high-risk alert immediately.</li>
 *
 *   <li><b>Refund Bursts</b>: If the number of high-value refunds (>= {@code amountThreshold})
 *       in a given {@code windowMillis} exceeds {@code countThreshold}, emit a burst alert.</li>
 *
 *   <li><b>Refunds Outside Shift</b>: Refunds performed outside cashier's scheduled shift hours
 *       increase the risk score (+0.2).</li>
 * </ul>
 *
 * <h3>State Managed:</h3>
 * <ul>
 *   <li>{@code recentRefunds} - List of recent {@link RefundEvent} within the analysis window.</li>
 *   <li>{@code escalated} - Boolean flag to avoid re-emitting burst alerts until conditions reset.</li>
 *   <li>{@code saleEvents} - Map of SALE transactionIds with TTL, used to validate refunds.</li>
 * </ul>
 *
 * <h3>Risk Score Calculation:</h3>
 * <pre>
 * Risk Score = min(1.0,
 *    (highCount / countThreshold) * 0.7     // volume factor (70%)
 *  + (avgRefundAmount / (2 * amountThreshold)) * 0.3   // amount factor (30%)
 * )
 *
 * Example:
 *   highCount=4, countThreshold=3,
 *   avgAmount=250, amountThreshold=100
 *
 *   volume_factor = (4/3)*0.7 = 0.933
 *   amount_factor = min(1.0, 250/200)*0.3 = 0.3
 *   score = min(1.0, 0.933+0.3) = 1.0
 * </pre>
 *
 * <h3>Timers:</h3>
 * <ul>
 *   <li>Registered for SALE events to expire them after {@code saleTtlMillis}.</li>
 *   <li>Registered after each REFUND to trigger pruning and escalation reset.</li>
 * </ul>
 *
 * <h3>Alerts:</h3>
 * <ul>
 *   <li>Refund Without Sale → Type = "REFUND_PATTERN", riskScore=1.0</li>
 *   <li>Refund Burst → Type = "REFUND_PATTERN", riskScore ∈ [0.0, 1.0]</li>
 * </ul>
 *
 * <h3>Side Outputs:</h3>
 * <ul>
 *   <li>{@link #AUDIT_OUTPUT} collects alerts for audit/logging purposes.</li>
 * </ul>
 *
 * <h3>Use Cases:</h3>
 * <ul>
 *   <li>Detecting refund fraud (cashiers refunding transactions that never occurred).</li>
 *   <li>Spotting suspicious bursts of high-value refunds in a short period.</li>
 *   <li>Flagging refunds performed outside assigned cashier shifts.</li>
 * </ul>
 */
@Slf4j
public class RefundPatternFunction extends KeyedProcessFunction<String, EnrichedPosEvent, AlertEvent> {

    public static final OutputTag<AlertEvent> AUDIT_OUTPUT = new OutputTag<>("refund-audit") {
    };

    private final double amountThreshold;
    private final int countThreshold;
    private final long windowMillis;
    private final long saleTtlMillis;

    private transient ListState<RefundEvent> recentRefunds;
    private transient ValueState<Boolean> escalated;
    private transient MapState<String, Long> saleEvents; // transactionId -> eventTime

    public RefundPatternFunction(double amountThreshold, int countThreshold, long windowMillis, long saleTtlMillis) {
        this.amountThreshold = amountThreshold;
        this.countThreshold = countThreshold;
        this.windowMillis = windowMillis;
        this.saleTtlMillis = saleTtlMillis;
    }

    @Override
    public void open(OpenContext context) {
        recentRefunds = getRuntimeContext().getListState(
                new ListStateDescriptor<>("recentRefunds", RefundEvent.class));
        escalated = getRuntimeContext().getState(
                new ValueStateDescriptor<>("escalated", Boolean.class));
        saleEvents = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("saleEvents", String.class, Long.class));
    }

    @Override
    public void processElement(EnrichedPosEvent enriched, Context ctx, Collector<AlertEvent> out) throws Exception {
        if (enriched == null) return;

        StaffShift shift = enriched.getShiftInfo();
        Instant eventTime = Instant.ofEpochMilli(enriched.getEventTime());
        boolean outsideShift = false;

        if (shift != null) {
            Instant shiftStart = Instant.ofEpochMilli(shift.getShiftStart());
            Instant shiftEnd = Instant.ofEpochMilli(shift.getShiftEnd());

            if (eventTime.isBefore(shiftStart) || eventTime.isAfter(shiftEnd)) {
                outsideShift = true;
            }
        }

        String eventType = enriched.getEventType();
        String txnId = enriched.getTransactionId();

        if ("SALE".equalsIgnoreCase(eventType)) {
            saleEvents.put(txnId, enriched.getEventTime());
            ctx.timerService().registerEventTimeTimer(enriched.getEventTime() + saleTtlMillis);
            return;
        }

        if (!"REFUND".equalsIgnoreCase(eventType)) {
            return;
        }

        if (!saleEvents.contains(txnId)) {
            emitRefundWithoutSaleAlert(enriched, out, ctx);
        }

        RefundEvent refund = RefundEvent.builder()
                .refundId("r-" + txnId)
                .transactionId(txnId)
                .storeId(enriched.getStoreId())
                .cashierId(enriched.getCashierId())
                .amount(enriched.getAmount())
                .eventTime(enriched.getEventTime())
                .build();

        recentRefunds.add(refund);
        pruneRefunds(enriched.getEventTime());

        long highCount = getHighValueCount();
        double avgAmount = getAverageRefundAmount();
        double score = computeRiskScore(highCount, avgAmount);

        // Increase risk score if refund outside shift
        if (outsideShift) {
            score = Math.min(1.0, score + 0.2);
            log.warn("Refund outside shift detected for cashier {} at store {}", enriched.getCashierId(), enriched.getStoreId());
        }

        Boolean already = escalated.value();
        if (highCount >= countThreshold && (already == null || !already)) {
            emitRefundBurstAlert(enriched, highCount, score, out, ctx);
            escalated.update(true);
        }

        ctx.timerService().registerEventTimeTimer(enriched.getEventTime() + 1L);
    }

    private void pruneRefunds(long currentEventTime) throws Exception {
        long cutoff = currentEventTime - windowMillis;
        List<RefundEvent> kept = new ArrayList<>();
        for (RefundEvent r : recentRefunds.get()) {
            if (r.getEventTime() >= cutoff) kept.add(r);
        }
        recentRefunds.update(kept);
    }

    private long getHighValueCount() throws Exception {
        List<RefundEvent> list = new ArrayList<>();
        for (RefundEvent r : recentRefunds.get()) {
            list.add(r);
        }
        return list.stream().filter(r -> r.getAmount() >= amountThreshold).count();
    }

    private double getAverageRefundAmount() throws Exception {
        List<RefundEvent> list = new ArrayList<>();
        for (RefundEvent r : recentRefunds.get()) {
            list.add(r);
        }
        return list.stream().mapToDouble(RefundEvent::getAmount).average().orElse(0.0);
    }

    //Risk score = 70% based on how many suspicious refunds we’ve seen,
    //plus 30% based on how big those refunds are.
    //Cap the final score at 1.0.

    //highCount = 4
    //countThreshold = 3
    //avgAmount = 250
    //amountThreshold = 100

    //volume_factor = (4 / 3) * 0.7 = 0.933...
    //amount_factor = min(1.0, 250 / 200) * 0.3 = (1.0) * 0.3 = 0.3
    //score = min(1.0, 0.933 + 0.3) = 1.0

    // Risk = 100%
    private double computeRiskScore(long highCount, double avgAmount) {
        return Math.min(1.0, (highCount / (double) Math.max(1, countThreshold)) * 0.7
                + Math.min(1.0, avgAmount / (amountThreshold * 2)) * 0.3);
    }

    private void emitRefundWithoutSaleAlert(EnrichedPosEvent enriched, Collector<AlertEvent> out, Context ctx) {
        AlertEvent alert = AlertEvent.builder()
                .alertId(UUID.randomUUID().toString())
                .storeId(enriched.getStoreId())
                .employId(enriched.getCashierId())
                .type("REFUND_PATTERN")
                .message(String.format("refund_without_sale: transaction %s", enriched.getTransactionId()))
                .riskScore(1.0)
                .alertTime(Instant.now())
                .build();
        out.collect(alert);
        ctx.output(AUDIT_OUTPUT, alert);
        log.info("Refund without sale detected: {}", enriched.getTransactionId());
    }

    private void emitRefundBurstAlert(EnrichedPosEvent enriched, long highCount, double score,
                                      Collector<AlertEvent> out, Context ctx) {
        AlertEvent alert = AlertEvent.builder()
                .alertId(UUID.randomUUID().toString())
                .storeId(enriched.getStoreId())
                .employId(enriched.getCashierId())
                .type("REFUND_PATTERN")
                .message(String.format("refund_burst: %d refunds >= %.2f in %d ms", highCount, amountThreshold, windowMillis))
                .riskScore(score)
                .alertTime(Instant.now())
                .build();
        out.collect(alert);
        ctx.output(AUDIT_OUTPUT, alert);
        log.info("Refund burst detected for cashier {}: {}", enriched.getCashierId(), highCount);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AlertEvent> out) throws Exception {
        // Prune refund list
        pruneRefunds(timestamp);

        // Cleanup expired sale records
        Iterator<Map.Entry<String, Long>> iter = saleEvents.entries().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Long> entry = iter.next();
            if (entry.getValue() + saleTtlMillis <= timestamp) {
                iter.remove();
            }
        }

        // Reset escalation if below threshold
        if (getHighValueCount() < countThreshold) {
            escalated.clear();
        }
    }
}
