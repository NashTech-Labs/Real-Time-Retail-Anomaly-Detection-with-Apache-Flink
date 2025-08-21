package com.nashtech.demo.flink.sink;

import com.nashtech.demo.flink.model.AlertEvent;
import com.nashtech.demo.flink.serdeser.GenericJsonSeralizeDeserialize;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * KafkaAlertSink is a reusable utility to write {@link AlertEvent} objects into a Kafka topic.
 *
 * <h3>Usage:</h3>
 * <pre>{@code
 * KafkaAlertSink.applyKafkaSink(alertStream, "localhost:9092", "alerts-topic");
 * }</pre>
 *
 * <h3>Behavior:</h3>
 * <ul>
 *   <li>Serializes {@link AlertEvent} as JSON using {@link  GenericJsonSeralizeDeserialize}.</li>
 *   <li>Publishes records to the configured Kafka topic.</li>
 *   <li>Uses delivery guarantee {@code AT_LEAST_ONCE} for reliability.</li>
 *   <li>Sink is named <b>ANOMALIES-KAFKA-WRITER</b> for easier job graph inspection in Flink UI.</li>
 * </ul>
 *
 * <h3>Parameters:</h3>
 * <ul>
 *   <li><b>stream</b> — DataStream of {@link AlertEvent} (produced by anomaly detection functions).</li>
 *   <li><b>brokers</b> — Comma-separated Kafka bootstrap servers (e.g., "broker1:9092,broker2:9092").</li>
 *   <li><b>topic</b> — Kafka topic name where alerts will be written.</li>
 * </ul>
 *
 * <h3>Delivery Semantics:</h3>
 * <ul>
 *   <li>Configured as <b>AT_LEAST_ONCE</b>: ensures alerts are never lost, but duplicates may occur
 *       in case of retries or task failures.</li>
 *   <li>Change to <b>EXACTLY_ONCE</b> if using Kafka transactions and the use case demands stronger guarantees.</li>
 * </ul>
 *
 * <h3>Example Flow:</h3>
 * <pre>
 * AlertEvent -> Flink Anomaly Detection -> KafkaAlertSink -> Kafka Topic ("alerts")
 * </pre>
 *
 * <h3>Integration:</h3>
 * <ul>
 *   <li>Downstream systems (Kafka Connect → InfluxDB → Grafana) can consume alerts from this topic.</li>
 *   <li>Multiple anomaly detection functions (refund, shrinkage, etc.) can share the same sink or use different topics.</li>
 * </ul>
 */
public class KafkaAlertSink {

    public static void applyKafkaSink(
            DataStream<AlertEvent> stream, String brokers, String topic) {

        KafkaSink<AlertEvent> sink = KafkaSink.<AlertEvent>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new GenericJsonSeralizeDeserialize<>(AlertEvent.class))
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        stream.sinkTo(sink).name("ANOMALIES-KAFKA-WRITER");
    }
}
