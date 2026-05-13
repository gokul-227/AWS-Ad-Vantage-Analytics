package com.adanalytics.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Objects;

/**
 * Represents an ad click event — generated when a user clicks on an advertisement.
 *
 * <p>A click event shares the same ID as its corresponding view event,
 * allowing us to join them in the Flink pipeline. The click timestamp
 * is always >= the view timestamp (you can't click before you see).</p>
 *
 * <p>Example JSON:
 * <pre>
 * {
 *   "ID": 1,
 *   "AdName": "Happy Bird",
 *   "SiteName": "Google",
 *   "Time": 1680307260000
 * }
 * </pre>
 * </p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClickEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    /** ID matching the corresponding ViewEvent */
    @JsonProperty("ID")
    private long id;

    @JsonProperty("AdName")
    private String adName;

    @JsonProperty("SiteName")
    private String siteName;

    /** Unix timestamp in milliseconds when the click occurred */
    @JsonProperty("Time")
    private long time;

    public ClickEvent() {}

    public ClickEvent(long id, String adName, String siteName, long time) {
        this.id = id;
        this.adName = adName;
        this.siteName = siteName;
        this.time = time;
    }

    public long getId() { return id; }
    public void setId(long id) { this.id = id; }

    public String getAdName() { return adName; }
    public void setAdName(String adName) { this.adName = adName; }

    public String getSiteName() { return siteName; }
    public void setSiteName(String siteName) { this.siteName = siteName; }

    public long getTime() { return time; }
    public void setTime(long time) { this.time = time; }

    public String getAdSiteKey() {
        return adName + "|" + siteName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClickEvent that = (ClickEvent) o;
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
        return String.format("ClickEvent{id=%d, ad='%s', site='%s', time=%d}",
                id, adName, siteName, time);
    }
}
