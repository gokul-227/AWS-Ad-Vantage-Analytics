package com.adanalytics.function;

import com.adanalytics.model.MetricResult;
import com.adanalytics.model.ViewWithClicks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Counts views that have at least one click within a 2-hour sliding window.
 *
 * <p><b>Metric: VIEWS_WITH_CLICKS</b> — Mandatory metric #3</p>
 *
 * <p><b>Flink Concept - Sliding Window:</b>
 * A sliding window has two parameters:
 * <ul>
 *   <li><b>Size</b>: How much time the window covers (2 hours here)</li>
 *   <li><b>Slide</b>: How often a new window starts (5 minutes here)</li>
 * </ul>
 * With a 2-hour size and 5-minute slide, each event belongs to
 * 2h / 5min = 24 overlapping windows. Every 5 minutes, Flink computes
 * the count of views-with-clicks over the last 2 hours.
 *
 * <p>Visually:
 * <pre>
 * |-- Window 1 (00:00 - 02:00) --|
 *      |-- Window 2 (00:05 - 02:05) --|
 *           |-- Window 3 (00:10 - 02:10) --|
 * </pre>
 * </p>
 *
 * <p><b>Sliding vs Tumbling:</b>
 * - Sliding windows OVERLAP — events belong to multiple windows
 * - Tumbling windows DON'T overlap — each event belongs to exactly one window
 * </p>
 *
 * <p><b>ProcessWindowFunction vs AggregateFunction:</b>
 * ProcessWindowFunction gives access to ALL elements in the window at once
 * (higher memory usage) plus window metadata (start/end times).
 * AggregateFunction processes elements incrementally (lower memory).
 * We use ProcessWindowFunction here because we need the window times
 * and the dataset fits in memory.
 * </p>
 */
public class ViewsWithClicksWindowFunction
        extends ProcessWindowFunction<ViewWithClicks, MetricResult, String, TimeWindow> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ViewsWithClicksWindowFunction.class);

    @Override
    public void process(String key, Context context,
                        Iterable<ViewWithClicks> elements,
                        Collector<MetricResult> out) {

        long viewsWithClicks = 0;
        long totalViews = 0;

        for (ViewWithClicks vwc : elements) {
            totalViews++;
            if (vwc.hasClicks()) {
                viewsWithClicks++;
            }
        }

        String[] parts = key.split("\\|");
        String adName = parts[0];
        String siteName = parts[1];

        TimeWindow window = context.window();

        MetricResult result = MetricResult.windowed(
                "VIEWS_WITH_CLICKS",
                adName,
                siteName,
                viewsWithClicks,
                System.currentTimeMillis(),
                window.getStart(),
                window.getEnd()
        );
        result.addDetail("totalViewsInWindow", totalViews);
        result.addDetail("clickRate", totalViews > 0 ?
                (double) viewsWithClicks / totalViews : 0.0);

        out.collect(result);

        LOG.debug("Window [{}-{}] {}: {}/{} views had clicks",
                window.getStart(), window.getEnd(), key, viewsWithClicks, totalViews);
    }
}
