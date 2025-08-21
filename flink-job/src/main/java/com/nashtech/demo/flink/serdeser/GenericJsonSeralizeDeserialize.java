package com.nashtech.demo.flink.serdeser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.IOException;

public class GenericJsonSeralizeDeserialize<T> implements SerializationSchema<T>, DeserializationSchema<T> {

    private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private final Class<T> clazz;

    public GenericJsonSeralizeDeserialize(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public byte[] serialize(T element) {
        try {
            return mapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        return mapper.readValue(message, clazz);
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public org.apache.flink.api.common.typeinfo.TypeInformation<T> getProducedType() {
        return org.apache.flink.api.java.typeutils.TypeExtractor.getForClass(clazz);
    }
}