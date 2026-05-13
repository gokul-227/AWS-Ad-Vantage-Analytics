package com.adanalytics.function;

import com.adanalytics.model.MetricResult;
import com.adanalytics.model.ViewEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Computes running total of views per ad+site combination.
 *
 * <p><b>Metric: TOTAL_VIEWS</b> — Mandatory metric #1</p>
 *
 * <p><b>Flink Concept - KeyedProcessFunction:</b>
 * A KeyedProcessFunction processes elements one at a time on a keyed stream.
 * Because the stream is keyed (by ad+site), each key gets its own isolated
 * state. This means the counter for "Happy Bird|Google" is completely separate
 * from "Ocean Breeze|Facebook".
 * </p>
 *
 * <p><b>Flink Concept - ValueState:</b>
 * ValueState is the simplest form of keyed state — it stores a single value
 * per key. Flink automatically manages this state:
 * <ul>
 *   <li>Persists it across restarts (via checkpoints)</li>
 *   <li>Partitions it by key</li>
 *   <li>Cleans it up when keys expire</li>
 * </ul>
 * Other state types: ListState, MapState, ReducingState, AggregatingState.
 * </p>
 *
 * <p><b>How this differs from Spark:</b>
 * In Spark Structured Streaming, you'd use a streaming aggregation with
 * groupBy().count(). Under the hood, Spark uses micro-batches and state stores.
 * Flink processes each event immediately with sub-second latency, while Spark
 * typically has latency equal to the micro-batch interval (100ms-seconds).
 * </p>
 */
public class TotalViewCountFunction extends KeyedProcessFunction<String, ViewEvent, MetricResult> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TotalViewCountFunction.class);

    /**
     * Keyed state holding the running view count.
     * Each key (ad+site combination) has its own counter.
     */
    private transient ValueState<Long> viewCount;

    /**
     * Called once when the function is initialized on a TaskManager.
     * This is where we register our state descriptors.
     *
     * <p><b>Flink Concept - State Registration:</b>
     * State must be registered in open() using descriptors. The descriptor
     * defines the state name (used for savepoint compatibility) and the
     * data type. If you rename state, you lose backward compatibility
     * with existing savepoints.</p>
     */
    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                "total-view-count",  // State name — used in savepoints
                Types.LONG           // State data type
        );
        viewCount = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(ViewEvent event, Context ctx, Collector<MetricResult> out) throws Exception {
        // Get current count (null on first event for this key)
        Long currentCount = viewCount.value();
        long newCount = (currentCount == null) ? 1L : currentCount + 1;

        // Update state
        viewCount.update(newCount);

        // Emit updated metric
        String[] parts = ctx.getCurrentKey().split("\\|");
        String adName = parts[0];
        String siteName = parts[1];

        out.collect(MetricResult.counter(
                "TOTAL_VIEWS", adName, siteName, newCount, event.getTime()));

        if (newCount % 50 == 0) {
            LOG.info("Total views for {}|{}: {}", adName, siteName, newCount);
        }
    }
}
