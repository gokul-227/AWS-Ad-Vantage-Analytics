package com.adanalytics.function;

import com.adanalytics.model.MetricResult;
import com.adanalytics.model.ViewEvent;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Counts views within a 2-hour sliding event-time window.
 *
 * <p><b>Metric: SLIDING_VIEWS_PER_AD_SITE / SLIDING_VIEWS_PER_AD</b>
 * — Additional metric #2</p>
 *
 * <p>This function is reused for two variations:
 * <ul>
 *   <li>Per ad AND site (keyed by "adName|siteName")</li>
 *   <li>Per ad only (keyed by "adName")</li>
 * </ul>
 * The metricName constructor parameter differentiates them.</p>
 *
 * <p><b>Flink Concept - Event Time vs Processing Time Windows:</b>
 * We use event-time windows (SlidingEventTimeWindows) which means the
 * window boundaries are based on the event timestamps, NOT when Flink
 * processes the events. This is crucial for correctness:
 * <ul>
 *   <li>If events arrive late (out of order), they still go to the correct window</li>
 *   <li>Results are deterministic — replaying the same events gives same results</li>
 *   <li>Processing-time windows would give different results on each replay</li>
 * </ul>
 * </p>
 */
public class SlidingViewCountFunction
        extends ProcessWindowFunction<ViewEvent, MetricResult, String, TimeWindow> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SlidingViewCountFunction.class);

    private final String metricName;

    /**
     * @param metricName Identifies the metric variant
     *                   (SLIDING_VIEWS_PER_AD_SITE or SLIDING_VIEWS_PER_AD)
     */
    public SlidingViewCountFunction(String metricName) {
        this.metricName = metricName;
    }

    @Override
    public void process(String key, Context context,
                        Iterable<ViewEvent> elements,
                        Collector<MetricResult> out) {
        long count = 0;
        for (ViewEvent ignored : elements) {
            count++;
        }

        TimeWindow window = context.window();

        // Parse key — might be "ad|site" or just "ad"
        String adName;
        String siteName;
        if (key.contains("|")) {
            String[] parts = key.split("\\|");
            adName = parts[0];
            siteName = parts[1];
        } else {
            adName = key;
            siteName = "ALL";
        }

        out.collect(MetricResult.windowed(
                metricName, adName, siteName, count,
                System.currentTimeMillis(),
                window.getStart(), window.getEnd()));

        LOG.debug("Window [{}-{}] {} {}: {} views",
                window.getStart(), window.getEnd(), metricName, key, count);
    }
}
