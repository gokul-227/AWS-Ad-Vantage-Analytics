package com.adanalytics.function;

import com.adanalytics.model.MetricResult;
import com.adanalytics.model.ViewWithClicks;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Computes the average number of clicks per view over a tumbling count window of 1000 views.
 *
 * <p><b>Metric: AVG_CLICKS_PER_VIEW</b> — Additional metric #3</p>
 *
 * <p><b>Flink Concept - AggregateFunction:</b>
 * AggregateFunction is an INCREMENTAL aggregation — it processes elements
 * one at a time and maintains a running accumulator, WITHOUT storing all
 * elements. This is much more memory-efficient than ProcessWindowFunction.
 *
 * <p>The three type parameters are:
 * <ul>
 *   <li><b>IN</b>: Input type (ViewWithClicks)</li>
 *   <li><b>ACC</b>: Accumulator type (Tuple2 of totalClicks, viewCount)</li>
 *   <li><b>OUT</b>: Output type (MetricResult)</li>
 * </ul>
 * </p>
 *
 * <p><b>Flink Concept - Count Window:</b>
 * Unlike time-based windows, a count window fires after a fixed number
 * of elements arrive. countWindow(1000) fires after every 1000 events
 * per key. Count windows are NOT based on event time and don't use
 * watermarks — they simply count elements.
 * </p>
 *
 * <p><b>Important:</b> Count windows only fire when the count is reached.
 * If a key receives only 500 events, the window never fires. This is
 * by design — in production, you'd add a processing-time trigger as
 * a fallback to emit partial results.
 * </p>
 */
public class AverageClicksAggregateFunction
        implements AggregateFunction<ViewWithClicks, Tuple2<Long, Long>, MetricResult> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AverageClicksAggregateFunction.class);

    /**
     * Create initial accumulator: (totalClicks=0, viewCount=0)
     */
    @Override
    public Tuple2<Long, Long> createAccumulator() {
        return Tuple2.of(0L, 0L);
    }

    /**
     * Add an element to the accumulator.
     * Called once per element — this is where the incremental logic lives.
     */
    @Override
    public Tuple2<Long, Long> add(ViewWithClicks value, Tuple2<Long, Long> accumulator) {
        return Tuple2.of(
                accumulator.f0 + value.getClickCount(),  // total clicks
                accumulator.f1 + 1                        // view count
        );
    }

    /**
     * Extract the final result from the accumulator.
     * Called when the window fires (after 1000 elements).
     */
    @Override
    public MetricResult getResult(Tuple2<Long, Long> accumulator) {
        double avgClicks = accumulator.f1 > 0
                ? (double) accumulator.f0 / accumulator.f1
                : 0.0;

        MetricResult result = new MetricResult(
                "AVG_CLICKS_PER_VIEW", "ALL", "ALL",
                avgClicks, System.currentTimeMillis());
        result.addDetail("totalClicks", accumulator.f0);
        result.addDetail("totalViews", accumulator.f1);

        return result;
    }

    /**
     * Merge two accumulators (used when windows from different partitions merge).
     *
     * <p><b>Flink Concept - Accumulator Merging:</b>
     * When using session windows or when Flink redistributes work,
     * accumulators from different sub-windows may need to be merged.
     * For count windows this is less common, but it's good practice
     * to always implement merge correctly.
     * </p>
     */
    @Override
    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
    }
}
