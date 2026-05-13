package com.adanalytics.function;

import com.adanalytics.model.MetricResult;
import com.adanalytics.model.ViewWithClicks;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Computes the Click-Through Rate (CTR) over time per ad+site.
 *
 * <p><b>Metric: CTR</b> — Additional metric #1</p>
 *
 * <p>CTR = total clicks / total views for each ad+site combination.
 * This is a running metric that updates with every new enriched view event.</p>
 *
 * <p><b>Flink Concept - Multiple State Variables:</b>
 * A single operator can maintain multiple state variables. Here we keep
 * separate counters for views and clicks, both keyed by ad+site.
 * Each key's state is isolated — updating "Happy Bird|Google" doesn't
 * affect "Ocean Breeze|Facebook".
 * </p>
 */
public class CTRFunction extends KeyedProcessFunction<String, ViewWithClicks, MetricResult> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CTRFunction.class);

    private transient ValueState<Long> totalViews;
    private transient ValueState<Long> totalClicks;

    @Override
    public void open(Configuration parameters) {
        totalViews = getRuntimeContext().getState(
                new ValueStateDescriptor<>("ctr-views", Types.LONG));
        totalClicks = getRuntimeContext().getState(
                new ValueStateDescriptor<>("ctr-clicks", Types.LONG));
    }

    @Override
    public void processElement(ViewWithClicks event, Context ctx,
                               Collector<MetricResult> out) throws Exception {
        Long views = totalViews.value();
        Long clicks = totalClicks.value();

        long newViews = (views == null ? 0L : views) + 1;
        long newClicks = (clicks == null ? 0L : clicks) + event.getClickCount();

        totalViews.update(newViews);
        totalClicks.update(newClicks);

        double ctr = newViews > 0 ? (double) newClicks / newViews : 0.0;

        String[] parts = ctx.getCurrentKey().split("\\|");
        String adName = parts[0];
        String siteName = parts[1];

        MetricResult result = new MetricResult(
                "CTR", adName, siteName, ctr, event.getViewTime());
        result.addDetail("totalViews", newViews);
        result.addDetail("totalClicks", newClicks);

        out.collect(result);
    }
}
