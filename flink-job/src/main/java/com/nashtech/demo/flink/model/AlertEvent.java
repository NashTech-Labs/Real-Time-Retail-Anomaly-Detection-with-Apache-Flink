package com.nashtech.demo.flink.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AlertEvent {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String alertId;
    private String storeId;
    private String employId;
    private String type; // e.g., "REFUND_PATTERN", "SHRINKAGE", "SECURITY_CORRELATION"
    private String message;
    private double riskScore;
    private Instant alertTime;

}