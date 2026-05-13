package com.adanalytics.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Generic JSON deserialization schema for Kafka events.
 *
 * <p><b>Flink Concept - DeserializationSchema:</b>
 * When Flink reads from Kafka, it receives raw bytes. A DeserializationSchema
 * converts those bytes into Java objects that Flink can process.
 * This generic implementation works for any POJO that Jackson can deserialize.</p>
 *
 * <p><b>Why not use Flink's built-in JSON format?</b>
 * We use a custom schema for better error handling (DLQ support),
 * logging, and to demonstrate the pattern used in production systems.</p>
 *
 * @param <T> The type to deserialize into (e.g., ViewEvent, ClickEvent)
 */
public class JsonDeserializationSchema<T> implements DeserializationSchema<T> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(JsonDeserializationSchema.class);

    private final Class<T> targetClass;
    private transient ObjectMapper objectMapper;

    public JsonDeserializationSchema(Class<T> targetClass) {
        this.targetClass = targetClass;
    }

    /**
     * Jackson ObjectMapper is NOT serializable, so we create it lazily.
     * The 'transient' keyword tells Java's serialization to skip this field.
     * Flink serializes operators and sends them to TaskManagers, so all
     * fields must be serializable or handled like this.
     */
    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            LOG.warn("Received null or empty message, skipping");
            return null;
        }
        try {
            return getObjectMapper().readValue(message, targetClass);
        } catch (IOException e) {
            // In production, you'd send this to a Dead Letter Queue (DLQ)
            LOG.error("Failed to deserialize message: {}. Error: {}",
                    new String(message), e.getMessage());
            // Return null — Flink will filter nulls if we handle it downstream
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        // Kafka streams are unbounded — they never end
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(targetClass);
    }
}
