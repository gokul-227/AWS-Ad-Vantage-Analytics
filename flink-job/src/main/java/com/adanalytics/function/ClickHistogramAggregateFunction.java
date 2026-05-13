package com.adanalytics.function;

import com.adanalytics.model.MetricResult;
import com.adanalytics.model.ViewWithClicks;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Computes a histogram of click counts per view over a tumbling count window of 1000 views.
 *
 * <p><b>Metric: CLICK_HISTOGRAM</b> — Additional metric #4</p>
 *
 * <p>The histogram shows the distribution of clicks per view:
 * <pre>
 * {
 *   "0_clicks": 650,    // 650 views had 0 clicks
 *   "1_click":  250,    // 250 views had exactly 1 click
 *   "2_clicks":  80,    // 80 views had 2 clicks
 *   "3_clicks":  15,    // 15 views had 3 clicks
 *   "4+_clicks":  5     // 5 views had 4 or more clicks
 * }
 * </pre>
 * </p>
 *
 * <p><b>Flink Concept - Custom Accumulators:</b>
 * The accumulator type can be any serializable Java object. Here we use
 * a HashMap to build the histogram incrementally. Each new element just
 * increments the appropriate bucket — O(1) per element.
 * </p>
 */
public class ClickHistogramAggregateFunction
        implements AggregateFunction<ViewWithClicks, Map<String, Long>, MetricResult> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ClickHistogramAggregateFunction.class);

    /** Maximum bucket before grouping into "N+" */
    private static final int MAX_BUCKET = 5;

    @Override
    public Map<String, Long> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public Map<String, Long> add(ViewWithClicks value, Map<String, Long> accumulator) {
        String bucket = getBucketName(value.getClickCount());
        accumulator.merge(bucket, 1L, Long::sum);
        return accumulator;
    }

    @Override
    public MetricResult getResult(Map<String, Long> accumulator) {
        // Sort by bucket name for consistent output
        Map<String, Object> sortedHistogram = new TreeMap<>();
        for (Map.Entry<String, Long> entry : accumulator.entrySet()) {
            sortedHistogram.put(entry.getKey(), entry.getValue());
        }

        long totalViews = accumulator.values().stream().mapToLong(Long::longValue).sum();

        MetricResult result = MetricResult.withDetails(
                "CLICK_HISTOGRAM", "ALL", "ALL",
                totalViews, System.currentTimeMillis(),
                sortedHistogram);

        return result;
    }

    @Override
    public Map<String, Long> merge(Map<String, Long> a, Map<String, Long> b) {
        Map<String, Long> merged = new HashMap<>(a);
        b.forEach((key, value) -> merged.merge(key, value, Long::sum));
        return merged;
    }

    /**
     * Maps a click count to a histogram bucket name.
     *
     * <p>Bucket scheme:
     * 0 → "0_clicks", 1 → "1_click", 2 → "2_clicks", ..., 5+ → "5+_clicks"
     * </p>
     */
    private String getBucketName(int clickCount) {
        if (clickCount == 0) {
            return "0_clicks";
        } else if (clickCount == 1) {
            return "1_click";
        } else if (clickCount < MAX_BUCKET) {
            return clickCount + "_clicks";
        } else {
            return MAX_BUCKET + "+_clicks";
        }
    }
}
