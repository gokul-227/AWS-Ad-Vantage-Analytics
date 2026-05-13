package com.adanalytics.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Objects;

/**
 * Represents an ad view event — generated when a user sees an advertisement.
 *
 * <p><b>Flink Concept - POJOs:</b>
 * Flink requires data types to be serializable. For best performance, Flink
 * uses its own serialization framework. A class is recognized as a Flink POJO if it:
 * <ul>
 *   <li>Is public and standalone (no non-static inner class)</li>
 *   <li>Has a public no-argument constructor</li>
 *   <li>All fields are public OR have public getters/setters</li>
 * </ul>
 * POJOs get efficient serialization and can be used as keys in keyBy() operations.
 * </p>
 *
 * <p>Example JSON:
 * <pre>
 * {
 *   "ID": 1,
 *   "AdName": "Happy Bird",
 *   "SiteName": "Google",
 *   "Time": 1680307200000
 * }
 * </pre>
 * </p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ViewEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Unique identifier for this view impression */
    @JsonProperty("ID")
    private long id;

    /** Name of the advertisement that was displayed */
    @JsonProperty("AdName")
    private String adName;

    /** Name of the website where the ad was shown */
    @JsonProperty("SiteName")
    private String siteName;

    /**
     * Unix timestamp in milliseconds when the view occurred.
     *
     * <p><b>Flink Concept - Event Time:</b>
     * This is the "event time" — the time when the event actually happened
     * in the real world. Flink distinguishes between:
     * <ul>
     *   <li><b>Event Time</b>: When the event occurred (this field)</li>
     *   <li><b>Processing Time</b>: When Flink processes the event</li>
     *   <li><b>Ingestion Time</b>: When the event enters Flink</li>
     * </ul>
     * We use event time because it gives correct results even when events
     * arrive late or out of order.
     * </p>
     */
    @JsonProperty("Time")
    private long time;

    /** Required no-arg constructor for Flink POJO serialization */
    public ViewEvent() {}

    public ViewEvent(long id, String adName, String siteName, long time) {
        this.id = id;
        this.adName = adName;
        this.siteName = siteName;
        this.time = time;
    }

    // Getters and Setters (required for Flink POJO recognition)
    public long getId() { return id; }
    public void setId(long id) { this.id = id; }

    public String getAdName() { return adName; }
    public void setAdName(String adName) { this.adName = adName; }

    public String getSiteName() { return siteName; }
    public void setSiteName(String siteName) { this.siteName = siteName; }

    public long getTime() { return time; }
    public void setTime(long time) { this.time = time; }

    /**
     * Composite key for grouping by ad and site.
     * Used in keyBy() operations throughout the pipeline.
     */
    public String getAdSiteKey() {
        return adName + "|" + siteName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ViewEvent that = (ViewEvent) o;
        return id == that.id && time == that.time
                && Objects.equals(adName, that.adName)
                && Objects.equals(siteName, that.siteName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, adName, siteName, time);
    }

    @Override
    public String toString() {
        return String.format("ViewEvent{id=%d, ad='%s', site='%s', time=%d}",
                id, adName, siteName, time);
    }
}
