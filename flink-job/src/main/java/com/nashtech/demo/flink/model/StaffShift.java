package com.nashtech.demo.flink.model;

import lombok.*;

/**
 * Represents a staff member's scheduled working shift.
 * Use case: Join with POS events to detect suspicious activity outside assigned shifts.
 */
@Data
public class StaffShift {
    private String employeeId;
    private String storeId;
    private long shiftStart;
    private long shiftEnd;
    private String role;
}