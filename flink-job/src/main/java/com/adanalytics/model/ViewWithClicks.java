package com.adanalytics.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Enriched view event that tracks how many clicks a particular view received.
 *
 * <p>This is produced by the {@code ViewClickJoinFunction} which performs a
 * stateful join between the view and click streams. Each ViewWithClicks
 * represents a single ad impression with its accumulated click count.</p>
 *
 * <p><b>Flink Concept - Stateful Stream Processing:</b>
 * To join two streams, Flink uses <b>keyed state</b> — state that is partitioned
 * by key. When we key both streams by event ID, Flink guarantees that all
 * events with the same ID go to the same operator instance. This allows us
 * to store a view in state and match it with incoming clicks.
 * </p>
 */
public class ViewWithClicks implements Serializable {
    private static final long serialVersionUID = 1L;

    private long viewId;
    private String adName;
    private String siteName;
    private long viewTime;
    private int clickCount;

    public ViewWithClicks() {}

    public ViewWithClicks(long viewId, String adName, String siteName, long viewTime, int clickCount) {
        this.viewId = viewId;
        this.adName = adName;
        this.siteName = siteName;
        this.viewTime = viewTime;
        this.clickCount = clickCount;
    }

    /** Create from a ViewEvent with initial click count of 0 */
    public static ViewWithClicks fromView(ViewEvent view) {
        return new ViewWithClicks(view.getId(), view.getAdName(), view.getSiteName(), view.getTime(), 0);
    }

    public void incrementClicks() {
        this.clickCount++;
    }

    public boolean hasClicks() {
        return clickCount > 0;
    }

    public String getAdSiteKey() {
        return adName + "|" + siteName;
    }

    // Getters and Setters
    public long getViewId() { return viewId; }
    public void setViewId(long viewId) { this.viewId = viewId; }

    public String getAdName() { return adName; }
    public void setAdName(String adName) { this.adName = adName; }

    public String getSiteName() { return siteName; }
    public void setSiteName(String siteName) { this.siteName = siteName; }

    public long getViewTime() { return viewTime; }
    public void setViewTime(long viewTime) { this.viewTime = viewTime; }

    public int getClickCount() { return clickCount; }
    public void setClickCount(int clickCount) { this.clickCount = clickCount; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ViewWithClicks that = (ViewWithClicks) o;
        return viewId == that.viewId && clickCount == that.clickCount
                && Objects.equals(adName, that.adName)
                && Objects.equals(siteName, that.siteName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(viewId, adName, siteName, clickCount);
    }

    @Override
    public String toString() {
        return String.format("ViewWithClicks{id=%d, ad='%s', site='%s', clicks=%d}",
                viewId, adName, siteName, clickCount);
    }
}
