package com.nashtech.demo.flink.functions;

import com.nashtech.demo.flink.model.*;

public class TestData {
    public static EnrichedPosEvent sale(String txnId, String store, String cashier, double amount, long ts) {
        EnrichedPosEvent e = new EnrichedPosEvent();
        e.setTransactionId(txnId);
        e.setStoreId(store);
        e.setCashierId(cashier);
        e.setAmount(amount);
        e.setEventTime(ts);
        e.setEventType("SALE");
        return e;
    }

    public static SecurityEvent security(String store, long ts, String gateId) {
        SecurityEvent s = new SecurityEvent();
        SecurityEvent.EventData eventData = new SecurityEvent.EventData();
        eventData.setAlarm(true);
        eventData.setGateId(gateId);
        s.setStoreId(store);
        s.setEventTime(ts);
        s.setEventData(eventData);
        return s;
    }

    public static EnrichedPosEvent sale(String txnId, String store, String cashier, String sku, double amount, long ts) {
        EnrichedPosEvent e = new EnrichedPosEvent();
        e.setTransactionId(txnId);
        e.setStoreId(store);
        e.setCashierId(cashier);
        e.setAmount(amount);
        e.setSku(sku);
        e.setEventTime(ts);
        e.setEventType("SALE");
        return e;
    }

    public static InventoryCount inventory(String store, String sku, String manager, long countedQty, long ts) {
        InventoryCount inv = new InventoryCount();
        inv.setStoreId(store);
        inv.setSku(sku);
        inv.setManagerId(manager);
        inv.setCountedQty(countedQty);
        inv.setEventTime(ts);
        return inv;
    }

    public static PosEvent pos(String txnId, String store, String cashier, String type,
                               double amount, String sku, long ts) {
        PosEvent p = new PosEvent();
        p.setTransactionId(txnId);
        p.setStoreId(store);
        p.setCashierId(cashier);
        p.setEventType(type);
        p.setAmount(amount);
        p.setSku(sku);
        p.setEventTime(ts);
        return p;
    }

    public static StaffShift shift(String empId, String store, long start, long end) {
        StaffShift s = new StaffShift();
        s.setEmployeeId(empId);
        s.setStoreId(store);
        s.setShiftStart(start);
        s.setShiftEnd(end);
        return s;
    }
}

