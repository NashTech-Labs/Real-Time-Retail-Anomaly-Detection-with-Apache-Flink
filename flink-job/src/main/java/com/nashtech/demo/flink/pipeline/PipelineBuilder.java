package com.nashtech.demo.flink.pipeline;

import com.nashtech.demo.flink.config.ConfigLoader;
import com.nashtech.demo.flink.functions.GateCorrelationFunction;
import com.nashtech.demo.flink.functions.InventoryShrinkageFunction;
import com.nashtech.demo.flink.functions.RefundPatternFunction;
import com.nashtech.demo.flink.functions.StaffShiftEnrichmentFunction;
import com.nashtech.demo.flink.model.*;
import com.nashtech.demo.flink.serdeser.GenericJsonSeralizeDeserialize;
import com.nashtech.demo.flink.sink.KafkaAlertSink;
import com.nashtech.demo.flink.source.KafkaSourceFactory;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.time.Duration;
import java.util.Properties;

public class PipelineBuilder implements Serializable {

    private final transient  StreamExecutionEnvironment env;
    private final transient ConfigLoader config;
    private final transient  KafkaSourceFactory kafkaSourceFactory;

    public PipelineBuilder(StreamExecutionEnvironment env) {
        this.env = env;
        this.config = ConfigLoader.getInstance();

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", config.getString("kafka.bootstrap"));
        kafkaProps.setProperty("group.id", config.getString("kafka.groupId"));

        this.kafkaSourceFactory = new KafkaSourceFactory(env, kafkaProps);
    }

    public void buildPipeline() {

        env.enableCheckpointing(30_000L, CheckpointingMode.EXACTLY_ONCE); // 30s
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000L);

        // Topics from config
        String posTopic = config.getString("kafka.topics.pos");
        String staffTopic = config.getString("kafka.topics.staff");
        String securityTopic = config.getString("kafka.topics.security");
        String inventoryTopic = config.getString("kafka.topics.inventory");

        // Thresholds from config
        double refundAmountThreshold = config.getDouble("thresholds.refund.amount");
        int refundCountThreshold = config.getInt("thresholds.refund.count");
        long refundWindowMs = config.getLong("thresholds.refund.windowMs");
        long saleTtlMs = config.getLong("thresholds.saleTtlMs");

        // Create sources
        DataStream<PosEvent> posStream = kafkaSourceFactory.createSource(
                posTopic,
                PosEvent::getEventTime,
                new GenericJsonSeralizeDeserialize<>(PosEvent.class));

        DataStream<StaffShift> staffStream = kafkaSourceFactory.createSource(
                staffTopic,
                StaffShift::getShiftStart,
                new GenericJsonSeralizeDeserialize<>(StaffShift.class));

        DataStream<SecurityEvent> securityStream = kafkaSourceFactory.createSource(
                securityTopic,
                SecurityEvent::getEventTime,
                new GenericJsonSeralizeDeserialize<>(SecurityEvent.class));

        DataStream<InventoryCount> inventoryStream = kafkaSourceFactory.createSource(
                inventoryTopic,
                InventoryCount::getEventTime,
                new GenericJsonSeralizeDeserialize<>(InventoryCount.class));

        KeyedStream<PosEvent, String> keyedPosStream =
                posStream.keyBy(e -> e.getStoreId() + ":" + e.getCashierId());
        // Broadcast staff shifts
        BroadcastStream<StaffShift> staffBroadcast =
                staffStream.broadcast(StaffShiftEnrichmentFunction.BROADCAST_STATE_DESC);

        // Enrich POS events with staff shift info
        DataStream<EnrichedPosEvent> enrichedPos = keyedPosStream
                .connect(staffBroadcast)
                .process(new StaffShiftEnrichmentFunction())
                .name("POS-ENRICHMENT");

        // Refund pattern detection
        DataStream<AlertEvent> refundAlerts = enrichedPos
                .keyBy(e -> e.getStoreId() + ":" + e.getCashierId())
                .process(new RefundPatternFunction(
                        refundAmountThreshold, refundCountThreshold, refundWindowMs, saleTtlMs))
                .name("REFUND-PATTERN-ANOMALIES-DETECTION");

        // Gate correlation alerts
        DataStream<AlertEvent> gateAlerts = enrichedPos
                .filter(e -> "SALE".equalsIgnoreCase(e.getEventType()))
                .keyBy(EnrichedPosEvent::getStoreId)
                .connect(securityStream.keyBy(SecurityEvent::getStoreId))
                .process(new GateCorrelationFunction(Duration.ofMinutes(5).toMillis(), 200.0))
                .name("SECURITY-GATE-ANOMALIES-DETECTION");

        // Inventory shrinkage alerts
        DataStream<AlertEvent> shrinkageAlerts = enrichedPos
                .filter(e -> "SALE".equalsIgnoreCase(e.getEventType()))
                .keyBy(e -> e.getStoreId() + ":" + e.getSku())
                .connect(inventoryStream.keyBy(i -> i.getStoreId() + ":" + i.getSku()))
                .process(new InventoryShrinkageFunction(5))
                .name("INVENTORY-SHRINKAGE-ANOMALIES-DETECTION");

        // Union all alerts and score
        DataStream<AlertEvent> allAlerts = refundAlerts.union(gateAlerts).union(shrinkageAlerts);

        KafkaAlertSink.applyKafkaSink(
                allAlerts,
                config.getString("kafka.bootstrap"),
                config.getString("kafka.topics.alertsTopic")
        );

    }
}
