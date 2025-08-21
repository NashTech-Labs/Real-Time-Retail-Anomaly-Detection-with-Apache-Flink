package com.nashtech.demo.flink.source;

import com.nashtech.demo.flink.serdeser.GenericJsonSeralizeDeserialize;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.time.Duration;
import java.util.Properties;

/**
 * Factory class for creating typed {@link DataStream} sources from Kafka topics.
 *
 * <h3>Features:</h3>
 * <ul>
 *   <li>Configurable for any event type via {@code GenericJsonSeralizeDeserialize<T>}.</li>
 *   <li>Applies {@link WatermarkStrategy} with 5s out-of-orderness tolerance.</li>
 *   <li>Allows caller to provide a custom timestamp extractor via {@link TimestampAssigner}.</li>
 *   <li>Supports multiple topics by calling {@link #createSource} repeatedly.</li>
 * </ul>
 *
 * <h3>Usage:</h3>
 * <pre>{@code
 * Properties kafkaProps = new Properties();
 * kafkaProps.put("bootstrap.servers", "localhost:9092");
 * kafkaProps.put("group.id", "pos-consumer");
 *
 * KafkaSourceFactory factory = new KafkaSourceFactory(env, kafkaProps);
 *
 * DataStream<PosEvent> posEvents = factory.createSource(
 *     "pos-events",
 *     PosEvent::getEventTime,
 *     new GenericJsonSeralizeDeserialize<>(PosEvent.class)
 * );
 *
 * DataStream<StaffShift> staffShifts = factory.createSource(
 *     "staff-shifts",
 *     StaffShift::getShiftStart,
 *     new GenericJsonSeralizeDeserialize<>(StaffShift.class)
 * );
 * }</pre>
 *
 * <h3>Parameters:</h3>
 * <ul>
 *   <li><b>topic</b> — Kafka topic name.</li>
 *   <li><b>timestampAssigner</b> — functional interface to extract event-time from T.</li>
 *   <li><b>deserializer</b> — JSON serializer/deserializer for type T.</li>
 * </ul>
 *
 * <h3>Notes:</h3>
 * <ul>
 *   <li>By default, source starts from <b>earliest</b> offsets.
 *       You can change this to {@code OffsetsInitializer.latest()} if desired.</li>
 *   <li>Group ID must be unique per consumer group.</li>
 *   <li>This factory is {@link Serializable}, but the Flink runtime
 *       will treat the {@link StreamExecutionEnvironment} and {@link Properties}
 *       as transient (not shipped to task managers).</li>
 * </ul>
 */
public class KafkaSourceFactory implements Serializable {

    private final transient StreamExecutionEnvironment env;
    private final transient Properties kafkaProps;

    public KafkaSourceFactory(StreamExecutionEnvironment env, Properties kafkaProps) {
        this.env = env;
        this.kafkaProps = kafkaProps;
    }

    public <T> DataStream<T> createSource(
            String topic,
            TimestampAssigner<T> timestampAssigner,
            GenericJsonSeralizeDeserialize<T> deserializer
    ) {
        // Build the KafkaSource
        KafkaSource<T> source = KafkaSource.<T>builder()
                .setBootstrapServers(kafkaProps.getProperty("bootstrap.servers"))
                .setTopics(topic)
                .setGroupId(kafkaProps.getProperty("group.id"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(deserializer)
                .build();

        return env.fromSource(source,
                WatermarkStrategy.<T>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, ts) -> timestampAssigner.getTimestamp(event)),
                topic + "-source");
    }

    @FunctionalInterface
    public interface TimestampAssigner<T> extends Serializable {
        long getTimestamp(T event);
    }
}

