package com.adanalytics.serialization;

import com.adanalytics.model.MetricResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Serializes MetricResult objects to Kafka ProducerRecords.
 *
 * <p><b>Flink Concept - KafkaRecordSerializationSchema:</b>
 * This is the new (post-1.17) API for serializing records to Kafka.
 * It replaces the older KafkaSerializationSchema and provides more
 * control over the output record (topic, key, value, headers).</p>
 *
 * <p>We use the metric name + ad + site as the Kafka record key.
 * This ensures that metrics for the same ad+site go to the same
 * Kafka partition, maintaining ordering for consumers.</p>
 */
public class MetricResultSerializationSchema implements KafkaRecordSerializationSchema<MetricResult> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MetricResultSerializationSchema.class);

    private final String topic;
    private transient ObjectMapper objectMapper;

    public MetricResultSerializationSchema(String topic) {
        this.topic = topic;
    }

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(MetricResult metric,
                                                     KafkaSinkContext context,
                                                     Long timestamp) {
        try {
            // Key by metric name + ad + site for partition affinity
            String key = metric.getMetricName() + "|" + metric.getAdName() + "|" + metric.getSiteName();
            byte[] value = getObjectMapper().writeValueAsBytes(metric);

            return new ProducerRecord<>(topic, key.getBytes(), value);
        } catch (JsonProcessingException e) {
            LOG.error("Failed to serialize metric: {}", metric, e);
            return null;
        }
    }
}
