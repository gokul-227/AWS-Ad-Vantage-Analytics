package com.adanalytics.function;

import com.adanalytics.model.ClickEvent;
import com.adanalytics.model.ViewEvent;
import com.adanalytics.model.ViewWithClicks;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Joins view and click streams to produce enriched ViewWithClicks events.
 *
 * <p><b>Flink Concept - CoProcessFunction:</b>
 * A CoProcessFunction processes two input streams simultaneously. When you
 * connect two keyed streams (keyed by the same key), Flink guarantees that
 * events with the same key from BOTH streams are processed by the same
 * operator instance. This enables stateful joins without shuffling data.
 * </p>
 *
 * <p><b>Join Strategy:</b>
 * <ol>
 *   <li>Both streams are keyed by event ID (view ID = click ID)</li>
 *   <li>When a view arrives: store it in state, set a 30-second timer</li>
 *   <li>When a click arrives: increment the click count in state</li>
 *   <li>When the timer fires: emit the ViewWithClicks and clear state</li>
 * </ol>
 * The 30-second window allows clicks that arrive shortly after views to be
 * matched. In production, you'd tune this based on expected click latency.
 * </p>
 *
 * <p><b>Why not use Flink's IntervalJoin?</b>
 * IntervalJoin produces one output per matching pair (view × click), which
 * means a view with 3 clicks produces 3 outputs. We want ONE output per
 * view with the total click count, so CoProcessFunction gives us more control.
 * </p>
 *
 * <p><b>Flink vs Spark Join:</b>
 * In Spark, stream-stream joins require defining a watermark and a time
 * range for the join. Flink's CoProcessFunction is more flexible — you
 * control exactly what state to keep and when to emit results.
 * </p>
 */
public class ViewClickJoinFunction extends CoProcessFunction<ViewEvent, ClickEvent, ViewWithClicks> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ViewClickJoinFunction.class);

    /**
     * How long to wait for clicks after a view arrives (in milliseconds).
     * Clicks arriving after this window are counted as "late" and the view
     * is already emitted. Configurable via environment variable.
     */
    private static final long JOIN_WINDOW_MS = Long.parseLong(
            System.getenv().getOrDefault("FLINK_JOIN_WINDOW_SECONDS", "30")) * 1000L;

    /**
     * State holding the current view with accumulated click count.
     * This is keyed by event ID, so each view impression has its own state.
     */
    private transient ValueState<ViewWithClicks> viewState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<ViewWithClicks> descriptor = new ValueStateDescriptor<>(
                "view-with-clicks",
                TypeInformation.of(ViewWithClicks.class)
        );
        viewState = getRuntimeContext().getState(descriptor);
    }

    /**
     * Process a view event from the first input stream.
     *
     * <p><b>Flink Concept - Timers:</b>
     * Timers allow you to schedule callbacks in the future. There are two types:
     * <ul>
     *   <li><b>Event-time timers</b>: Fire when the watermark passes the timer timestamp</li>
     *   <li><b>Processing-time timers</b>: Fire based on wall clock time</li>
     * </ul>
     * We use processing-time timers here because we want to emit results after
     * a fixed wall-clock duration, regardless of event-time progress.
     * </p>
     */
    @Override
    public void processElement1(ViewEvent view, Context ctx, Collector<ViewWithClicks> out) throws Exception {
        ViewWithClicks current = viewState.value();
        if (current == null) {
            // First time seeing this view ID — create new state
            ViewWithClicks vwc = ViewWithClicks.fromView(view);
            viewState.update(vwc);

            // Set a processing-time timer to emit the result after JOIN_WINDOW_MS
            long timerTimestamp = ctx.timerService().currentProcessingTime() + JOIN_WINDOW_MS;
            ctx.timerService().registerProcessingTimeTimer(timerTimestamp);

            LOG.debug("Stored view ID={}, timer set for {}ms", view.getId(), JOIN_WINDOW_MS);
        } else {
            // Duplicate view ID — this shouldn't normally happen
            LOG.warn("Duplicate view ID={}, ignoring", view.getId());
        }
    }

    /**
     * Process a click event from the second input stream.
     */
    @Override
    public void processElement2(ClickEvent click, Context ctx, Collector<ViewWithClicks> out) throws Exception {
        ViewWithClicks current = viewState.value();
        if (current != null) {
            // Found matching view — increment click count
            current.incrementClicks();
            viewState.update(current);
            LOG.debug("Click matched to view ID={}, total clicks={}", click.getId(), current.getClickCount());
        } else {
            // Click arrived before view or after timer — "orphaned click"
            // In production, you'd send this to a Dead Letter Queue
            LOG.debug("Orphaned click for ID={} (view not in state)", click.getId());
        }
    }

    /**
     * Timer callback — emit the enriched view and clear state.
     *
     * <p><b>Flink Concept - State Cleanup:</b>
     * It's critical to clear state when it's no longer needed. If you don't,
     * state grows unboundedly and eventually causes out-of-memory errors.
     * This is one of the most common mistakes in Flink applications.
     * </p>
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ViewWithClicks> out) throws Exception {
        ViewWithClicks result = viewState.value();
        if (result != null) {
            out.collect(result);
            LOG.debug("Emitted ViewWithClicks: id={}, clicks={}",
                    result.getViewId(), result.getClickCount());
        }
        // Clear state to prevent memory leaks
        viewState.clear();
    }
}
