package com.nashtech.demo.flink;

import com.nashtech.demo.flink.pipeline.PipelineBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AppMain {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        PipelineBuilder builder = new PipelineBuilder(env);
        builder.buildPipeline();

        env.execute("Retail Loss Prevention Job");
    }
}
