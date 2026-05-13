package com.adanalytics;

import com.adanalytics.function.*;
import com.adanalytics.model.*;
import com.adanalytics.serialization.JsonDeserializationSchema;
import com.adanalytics.serialization.MetricResultSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Main Flink streaming job for Ad Analytics.
 *
 * Pipeline: Kafka(views+clicks) -> Flink(7 metrics) -> Kafka(metrics-topic)
 *
 * Flink vs Spark: Flink is TRUE streaming (event-by-event), while Spark uses
 * micro-batches. Flink has native event-time support, lower latency, and
 * better exactly-once semantics.
 */
public class AdAnalyticsJob {
    private static final Logger LOG = LoggerFactory.getLogger(AdAnalyticsJob.class);

    public static void main(String[] args) throws Exception {
        // Configuration from environment
        String kafkaBootstrap = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String viewsTopic = getEnv("KAFKA_VIEWS_TOPIC", "views-topic");
        String clicksTopic = getEnv("KAFKA_CLICKS_TOPIC", "clicks-topic");
        String metricsTopic = getEnv("KAFKA_METRICS_TOPIC", "metrics-topic");
        int parallelism = Integer.parseInt(getEnv("FLINK_PARALLELISM", "2"));
        long checkpointInterval = Long.parseLong(getEnv("FLINK_CHECKPOINT_INTERVAL", "60000"));
        int watermarkDelay = Integer.parseInt(getEnv("FLINK_WATERMARK_DELAY_SECONDS", "10"));

        LOG.info("=== Ad Analytics Flink Job Starting ===");
        LOG.info("Kafka: {}, Parallelism: {}", kafkaBootstrap, parallelism);

        // --- Execution Environment ---
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(checkpointInterval);

        // --- Watermark Strategies ---
        // Watermarks tell Flink "all events up to this time have arrived."
        // BoundedOutOfOrderness allows events to be late by up to N seconds.
        WatermarkStrategy<ViewEvent> viewWatermark = WatermarkStrategy
                .<ViewEvent>forBoundedOutOfOrderness(Duration.ofSeconds(watermarkDelay))
                .withTimestampAssigner((event, ts) -> event.getTime())
                .withIdleness(Duration.ofMinutes(1));

        WatermarkStrategy<ClickEvent> clickWatermark = WatermarkStrategy
                .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(watermarkDelay))
                .withTimestampAssigner((event, ts) -> event.getTime())
                .withIdleness(Duration.ofMinutes(1));

        // --- Kafka Sources ---
        KafkaSource<ViewEvent> viewSource = KafkaSource.<ViewEvent>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics(viewsTopic)
                .setGroupId("flink-ad-analytics-views")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(ViewEvent.class))
                .build();

        KafkaSource<ClickEvent> clickSource = KafkaSource.<ClickEvent>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics(clicksTopic)
                .setGroupId("flink-ad-analytics-clicks")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(ClickEvent.class))
                .build();

        DataStream<ViewEvent> views = env
                .fromSource(viewSource, viewWatermark, "Kafka Views Source")
                .filter(v -> v != null).name("Filter Null Views");

        DataStream<ClickEvent> clicks = env
                .fromSource(clickSource, clickWatermark, "Kafka Clicks Source")
                .filter(c -> c != null).name("Filter Null Clicks");

        // === METRIC 1: Total Views per Ad+Site ===
        SingleOutputStreamOperator<MetricResult> totalViews = views
                .keyBy(ViewEvent::getAdSiteKey)
                .process(new TotalViewCountFunction())
                .name("Metric: Total Views");

        // === METRIC 2: Total Clicks per Ad+Site ===
        SingleOutputStreamOperator<MetricResult> totalClicks = clicks
                .keyBy(ClickEvent::getAdSiteKey)
                .process(new TotalClickCountFunction())
                .name("Metric: Total Clicks");

        // === JOIN: Enrich views with click counts ===
        DataStream<ViewWithClicks> enrichedViews = views
                .keyBy(ViewEvent::getId)
                .connect(clicks.keyBy(ClickEvent::getId))
                .process(new ViewClickJoinFunction())
                .name("Join: View-Click Enrichment")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ViewWithClicks>forBoundedOutOfOrderness(Duration.ofSeconds(watermarkDelay))
                                .withTimestampAssigner((event, ts) -> event.getViewTime())
                                .withIdleness(Duration.ofMinutes(1))
                );

        // === METRIC 3: Views with >=1 Click (2h sliding window) ===
        SingleOutputStreamOperator<MetricResult> viewsWithClicks = enrichedViews
                .keyBy(ViewWithClicks::getAdSiteKey)
                .window(SlidingEventTimeWindows.of(Time.hours(2), Time.minutes(5)))
                .process(new ViewsWithClicksWindowFunction())
                .name("Metric: Views With Clicks (2h Sliding)");

        // === METRIC 4: CTR ===
        SingleOutputStreamOperator<MetricResult> ctr = enrichedViews
                .keyBy(ViewWithClicks::getAdSiteKey)
                .process(new CTRFunction())
                .name("Metric: CTR");

        // === METRIC 5a: Views in 2h window per Ad+Site ===
        SingleOutputStreamOperator<MetricResult> slidingViewsAdSite = views
                .keyBy(ViewEvent::getAdSiteKey)
                .window(SlidingEventTimeWindows.of(Time.hours(2), Time.minutes(5)))
                .process(new SlidingViewCountFunction("SLIDING_VIEWS_PER_AD_SITE"))
                .name("Metric: Sliding Views (Ad+Site)");

        // === METRIC 5b: Views in 2h window per Ad only ===
        SingleOutputStreamOperator<MetricResult> slidingViewsAd = views
                .keyBy(ViewEvent::getAdName)
                .window(SlidingEventTimeWindows.of(Time.hours(2), Time.minutes(5)))
                .process(new SlidingViewCountFunction("SLIDING_VIEWS_PER_AD"))
                .name("Metric: Sliding Views (Ad Only)");

        // === METRIC 6: Average Clicks per 1000 Views (count window) ===
        SingleOutputStreamOperator<MetricResult> avgClicks = enrichedViews
                .keyBy(ViewWithClicks::getAdSiteKey)
                .countWindow(1000)
                .aggregate(new AverageClicksAggregateFunction())
                .name("Metric: Avg Clicks (1000 views)");

        // === METRIC 7: Click Histogram per 1000 Views ===
        SingleOutputStreamOperator<MetricResult> histogram = enrichedViews
                .keyBy(ViewWithClicks::getAdSiteKey)
                .countWindow(1000)
                .aggregate(new ClickHistogramAggregateFunction())
                .name("Metric: Click Histogram (1000 views)");

        // --- Kafka Sink ---
        KafkaSink<MetricResult> metricsSink = KafkaSink.<MetricResult>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setRecordSerializer(new MetricResultSerializationSchema(metricsTopic))
                .build();

        // Union all metrics and write to Kafka
        totalViews
                .union(totalClicks, viewsWithClicks, ctr,
                       slidingViewsAdSite, slidingViewsAd, avgClicks, histogram)
                .sinkTo(metricsSink)
                .name("Kafka Metrics Sink");

        LOG.info("=== Submitting Ad Analytics Pipeline ===");
        env.execute("Ad Analytics Streaming Pipeline");
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }
}
