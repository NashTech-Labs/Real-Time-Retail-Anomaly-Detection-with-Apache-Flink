package com.nashtech.demo.flink.source;

import com.nashtech.demo.flink.serdeser.GenericJsonSeralizeDeserialize;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class KafkaSourceFactoryTest {

    // Simple test event
    static class TestEvent implements Serializable {
        String id;
        long ts;

        TestEvent(String id, long ts) {
            this.id = id;
            this.ts = ts;
        }
    }

    @Test
    void testCreateSourceBuildsDataStream() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "dummy:9092");
        props.setProperty("group.id", "test-group");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSourceFactory factory = new KafkaSourceFactory(env, props);

        GenericJsonSeralizeDeserialize<TestEvent> serde =
                new GenericJsonSeralizeDeserialize<>(TestEvent.class);

        KafkaSourceFactory.TimestampAssigner<TestEvent> tsAssigner = e -> e.ts;

        // Call the factory (should not throw)
        DataStream<TestEvent> stream =
                factory.createSource("test-topic", tsAssigner, serde);

        assertNotNull(stream);
        assertEquals("test-topic-source", stream.getTransformation().getName());
    }

    @Test
    void testTimestampAssignerWorks() {
        long now = Instant.now().toEpochMilli();
        KafkaSourceFactory.TimestampAssigner<TestEvent> tsAssigner = e -> e.ts;

        TestEvent event = new TestEvent("1", now);

        long extracted = tsAssigner.getTimestamp(event);
        assertEquals(now, extracted);
    }

}
