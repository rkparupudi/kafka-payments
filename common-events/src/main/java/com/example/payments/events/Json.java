package com.example.payments.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;

/**
 * Minimal JSON helpers. Locally we use JSON for readability; production would use Avro or Protobuf
 * with Schema Registry (BACKWARD compatibility), as described in the design doc.
 */
public final class Json {

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false); // tolerate added fields

    private Json() {
    }

    public static String write(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot serialise " + value.getClass().getSimpleName(), e);
        }
    }

    public static <T> T read(String json, Class<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot deserialise " + type.getSimpleName(), e);
        }
    }

    /** Kafka Serde for a record type. Null payloads (tombstones) map to null. */
    public static <T> Serde<T> serde(Class<T> type) {
        Serializer<T> serializer = (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Cannot serialise " + type.getSimpleName(), e);
            }
        };
        Deserializer<T> deserializer = (topic, bytes) -> {
            if (bytes == null) {
                return null;
            }
            try {
                return MAPPER.readValue(bytes, type);
            } catch (IOException e) {
                throw new IllegalArgumentException("Cannot deserialise " + type.getSimpleName() + " from " + topic, e);
            }
        };
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
