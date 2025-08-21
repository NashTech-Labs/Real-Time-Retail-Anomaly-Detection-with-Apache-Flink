package com.nashtech.demo.flink.pipeline;

import com.nashtech.demo.flink.config.ConfigLoader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PipelineBuilderTest {

    private StreamExecutionEnvironment env;

    @BeforeEach
    void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
    }

    @Test
    void testBuildPipeline_addsExpectedOperators() {
        // mock ConfigLoader to return dummy config
        try (MockedStatic<ConfigLoader> mocked = mockStatic(ConfigLoader.class)) {
            ConfigLoader config = mock(ConfigLoader.class);
            mocked.when(ConfigLoader::getInstance).thenReturn(config);

            when(config.getString("kafka.bootstrap")).thenReturn("localhost:9092");
            when(config.getString("kafka.groupId")).thenReturn("test-group");

            when(config.getString("kafka.topics.pos")).thenReturn("pos-topic");
            when(config.getString("kafka.topics.staff")).thenReturn("staff-topic");
            when(config.getString("kafka.topics.security")).thenReturn("security-topic");
            when(config.getString("kafka.topics.inventory")).thenReturn("inventory-topic");
            when(config.getString("kafka.topics.alertsTopic")).thenReturn("alerts-topic");

            when(config.getDouble("thresholds.refund.amount")).thenReturn(100.0);
            when(config.getInt("thresholds.refund.count")).thenReturn(3);
            when(config.getLong("thresholds.refund.windowMs")).thenReturn(60000L);
            when(config.getLong("thresholds.saleTtlMs")).thenReturn(300000L);

            // build pipeline
            PipelineBuilder builder = new PipelineBuilder(env);
            builder.buildPipeline();

            // inspect graph
            StreamGraph graph = env.getStreamGraph();
            List<String> operatorNames = graph.getStreamNodes().stream()
                    .map(StreamNode::getOperatorName)
                    .toList();

            // assert pipeline has expected transformations
            assertTrue(operatorNames.stream().anyMatch(name -> name.contains("Source: pos-topic-source")));
            assertTrue(operatorNames.stream().anyMatch(name -> name.contains("Source: staff-topic-source")));
            assertTrue(operatorNames.stream().anyMatch(name -> name.contains("POS-ENRICHMENT")));
            assertTrue(operatorNames.stream().anyMatch(name -> name.contains("REFUND-PATTERN-ANOMALIES-DETECTION")));
            assertTrue(operatorNames.stream().anyMatch(name -> name.contains("SECURITY-GATE-ANOMALIES-DETECTION")));
            assertTrue(operatorNames.stream().anyMatch(name -> name.contains("INVENTORY-SHRINKAGE-ANOMALIES-DETECTION")));
            assertTrue(operatorNames.stream().anyMatch(name -> name.contains("ANOMALIES-KAFKA-WRITER")));
        }
    }
}
