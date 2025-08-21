package com.nashtech.demo.flink.sink;

import com.nashtech.demo.flink.model.AlertEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class KafkaAlertSinkTest {

    @Test
    void testApplyKafkaSink_doesNotThrow() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<AlertEvent> stream = env.fromElements(
                AlertEvent.builder()
                        .alertId("a1")
                        .employId("e1")
                        .storeId("s1")
                        .type("TEST")
                        .message("msg")
                        .riskScore(0.5)
                        .alertTime(Instant.now())
                        .build()
        );

        assertDoesNotThrow(() ->
                KafkaAlertSink.applyKafkaSink(stream, "localhost:9092", "test-topic")
        );
    }


}
