package com.adanalytics.function;

import com.adanalytics.model.ClickEvent;
import com.adanalytics.model.MetricResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Computes running total of clicks per ad+site combination.
 *
 * <p><b>Metric: TOTAL_CLICKS</b> — Mandatory metric #2</p>
 *
 * <p>Same pattern as TotalViewCountFunction but applied to the click stream.</p>
 */
public class TotalClickCountFunction extends KeyedProcessFunction<String, ClickEvent, MetricResult> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TotalClickCountFunction.class);

    private transient ValueState<Long> clickCount;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                "total-click-count", Types.LONG);
        clickCount = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(ClickEvent event, Context ctx, Collector<MetricResult> out) throws Exception {
        Long currentCount = clickCount.value();
        long newCount = (currentCount == null) ? 1L : currentCount + 1;
        clickCount.update(newCount);

        String[] parts = ctx.getCurrentKey().split("\\|");
        String adName = parts[0];
        String siteName = parts[1];

        out.collect(MetricResult.counter(
                "TOTAL_CLICKS", adName, siteName, newCount, event.getTime()));

        if (newCount % 50 == 0) {
            LOG.info("Total clicks for {}|{}: {}", adName, siteName, newCount);
        }
    }
}
