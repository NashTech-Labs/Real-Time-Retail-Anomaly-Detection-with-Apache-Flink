package com.nashtech.demo.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * Represents events from security systems (e.g., doors, sensors, alarms).
 * Use case: Drives correlation between suspicious refunds and unusual store activity.
 */

@Data
public class SecurityEvent {

    private String eventId;
    private String storeId;
    private long eventTime;
    private String eventType; // e.g., "DOOR_OPEN", "ALARM_TRIGGER"
    private EventData eventData;

    @Data
    public static class EventData {

        private String gateId;
        private boolean alarm;
    }
}