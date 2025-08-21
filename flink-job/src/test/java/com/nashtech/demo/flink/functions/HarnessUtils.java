package com.nashtech.demo.flink.functions;

import com.nashtech.demo.flink.model.*;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;

import java.util.ArrayList;
import java.util.List;

public class HarnessUtils {

    public static List<AlertEvent> getSecurityOutput(
            KeyedTwoInputStreamOperatorTestHarness<String, EnrichedPosEvent, SecurityEvent, AlertEvent> harness) {
        return new ArrayList<>(harness.extractOutputValues());
    }

    public static List<AlertEvent> getInventoryShrinkageOutput(
            KeyedTwoInputStreamOperatorTestHarness<String, EnrichedPosEvent, InventoryCount, AlertEvent> harness) {
        return new ArrayList<>(harness.extractOutputValues());
    }

    public static List<EnrichedPosEvent> getEnrichedPos(
            KeyedBroadcastOperatorTestHarness<String, PosEvent, StaffShift, EnrichedPosEvent> harness) {
        return new ArrayList<>(harness.extractOutputValues());
    }
}

