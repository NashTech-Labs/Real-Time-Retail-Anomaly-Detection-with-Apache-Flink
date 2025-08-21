package com.nashtech.demo.flink.functions;

import com.nashtech.demo.flink.model.EnrichedPosEvent;
import com.nashtech.demo.flink.model.PosEvent;
import com.nashtech.demo.flink.model.StaffShift;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * StaffShiftEnrichmentFunction enriches incoming {@link PosEvent} with staff shift information
 * provided via a broadcast stream of {@link StaffShift}.
 *
 * <p>This is a typical broadcast join pattern in Flink:</p>
 *
 * <ul>
 *   <li><b>Main keyed stream:</b> {@link PosEvent} keyed by {@code cashierId} (or store:cashierId).</li>
 *   <li><b>Broadcast stream:</b> {@link StaffShift} updates keyed by {@code employeeId}, broadcast
 *       to all parallel subtasks.</li>
 *   <li><b>Output:</b> {@link EnrichedPosEvent} containing both event and shift metadata.</li>
 * </ul>
 *
 * <h3>Behavior:</h3>
 * <ul>
 *   <li>When a {@link PosEvent} arrives:
 *     <ul>
 *       <li>Lookup the cashier’s current {@link StaffShift} from the broadcast state (if available).</li>
 *       <li>Attach it to the {@link EnrichedPosEvent}.</li>
 *       <li>Emit the enriched record downstream.</li>
 *     </ul>
 *   </li>
 *
 *   <li>When a {@link StaffShift} update arrives:
 *     <ul>
 *       <li>Store it in the broadcast state, keyed by {@code employeeId}.</li>
 *       <li>Newly arriving POS events will use the updated shift.</li>
 *       <li>Logs update events for observability.</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * <h3>State:</h3>
 * <ul>
 *   <li><b>Broadcast State:</b> {@code MapState<String, StaffShift>} where key = employeeId.</li>
 *   <li>No keyed state is maintained.</li>
 * </ul>
 *
 * <h3>Example:</h3>
 * <pre>
 * Input POS Event:  { cashierId="c123", eventType="REFUND", store="s1" }
 * Current Shift:    { employeeId="c123", shiftStart=09:00, shiftEnd=17:00 }
 *
 * Output Enriched Event:
 *   { cashierId="c123", eventType="REFUND", store="s1", shiftInfo={09:00-17:00} }
 * </pre>
 *
 * <h3>Use Cases:</h3>
 * <ul>
 *   <li>Fraud detection (e.g., refunds outside shift hours).</li>
 *   <li>Operational monitoring (e.g., workload per shift).</li>
 *   <li>Data quality enrichment for downstream analytics.</li>
 * </ul>
 */
@Slf4j
public class StaffShiftEnrichmentFunction
        extends KeyedBroadcastProcessFunction<String, PosEvent, StaffShift, EnrichedPosEvent> {

    public static final MapStateDescriptor<String, StaffShift> BROADCAST_STATE_DESC =
            new MapStateDescriptor<>("staffShifts", String.class, StaffShift.class);

    @Override
    public void processElement(PosEvent posEvent,
                               ReadOnlyContext ctx,
                               Collector<EnrichedPosEvent> out) throws Exception {

        ReadOnlyBroadcastState<String, StaffShift> broadcastState =
                ctx.getBroadcastState(BROADCAST_STATE_DESC);

        StaffShift shift = broadcastState.get(posEvent.getCashierId());

        EnrichedPosEvent enriched = EnrichedPosEvent.builder()
                .transactionId(posEvent.getTransactionId())
                .storeId(posEvent.getStoreId())
                .cashierId(posEvent.getCashierId())
                .eventType(posEvent.getEventType())
                .amount(posEvent.getAmount())
                .sku(posEvent.getSku())
                .eventTime(posEvent.getEventTime())
                .shiftInfo(shift)
                .watchlistFlag(false)
                .build();

        out.collect(enriched);
    }

    @Override
    public void processBroadcastElement(StaffShift shift,
                                        Context ctx,
                                        Collector<EnrichedPosEvent> out) throws Exception {
        BroadcastState<String, StaffShift> broadcastState =
                ctx.getBroadcastState(BROADCAST_STATE_DESC);

        broadcastState.put(shift.getEmployeeId(), shift);
        log.info("Updated shift for staff {} at store {}", shift.getEmployeeId(), shift.getStoreId());
    }
}

