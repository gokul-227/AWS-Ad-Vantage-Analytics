package com.adanalytics.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Unified output model for all computed metrics.
 *
 * <p>All seven metrics produce MetricResult objects that are serialized to JSON
 * and written to the Kafka metrics-topic. The {@code metricName} field identifies
 * which metric this result represents.</p>
 *
 * <p>Example output:
 * <pre>
 * {
 *   "metricName": "TOTAL_VIEWS",
 *   "adName": "Happy Bird",
 *   "siteName": "Google",
 *   "value": 42.0,
 *   "timestamp": 1680307200000,
 *   "windowStart": null,
 *   "windowEnd": null,
 *   "details": {}
 * }
 * </pre>
 * </p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MetricResult implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Metric type identifier (e.g., TOTAL_VIEWS, CTR, CLICK_HISTOGRAM) */
    @JsonProperty("metricName")
    private String metricName;

    @JsonProperty("adName")
    private String adName;

    @JsonProperty("siteName")
    private String siteName;

    /** Primary numeric value of the metric */
    @JsonProperty("value")
    private double value;

    /** Event time of the metric computation */
    @JsonProperty("timestamp")
    private long timestamp;

    /** Window start time (for windowed metrics) */
    @JsonProperty("windowStart")
    private Long windowStart;

    /** Window end time (for windowed metrics) */
    @JsonProperty("windowEnd")
    private Long windowEnd;

    /**
     * Additional key-value details (e.g., histogram buckets).
     * This allows flexible output without changing the schema.
     */
    @JsonProperty("details")
    private Map<String, Object> details;

    public MetricResult() {
        this.details = new HashMap<>();
    }

    public MetricResult(String metricName, String adName, String siteName,
                        double value, long timestamp) {
        this.metricName = metricName;
        this.adName = adName;
        this.siteName = siteName;
        this.value = value;
        this.timestamp = timestamp;
        this.details = new HashMap<>();
    }

    /** Factory for running counter metrics (no window) */
    public static MetricResult counter(String metricName, String adName, String siteName,
                                       long count, long timestamp) {
        return new MetricResult(metricName, adName, siteName, count, timestamp);
    }

    /** Factory for windowed metrics */
    public static MetricResult windowed(String metricName, String adName, String siteName,
                                        double value, long timestamp,
                                        long windowStart, long windowEnd) {
        MetricResult result = new MetricResult(metricName, adName, siteName, value, timestamp);
        result.setWindowStart(windowStart);
        result.setWindowEnd(windowEnd);
        return result;
    }

    /** Factory for metrics with additional details (histograms, etc.) */
    public static MetricResult withDetails(String metricName, String adName, String siteName,
                                           double value, long timestamp,
                                           Map<String, Object> details) {
        MetricResult result = new MetricResult(metricName, adName, siteName, value, timestamp);
        result.setDetails(details);
        return result;
    }

    public void addDetail(String key, Object val) {
        if (this.details == null) {
            this.details = new HashMap<>();
        }
        this.details.put(key, val);
    }

    // Getters and Setters
    public String getMetricName() { return metricName; }
    public void setMetricName(String metricName) { this.metricName = metricName; }

    public String getAdName() { return adName; }
    public void setAdName(String adName) { this.adName = adName; }

    public String getSiteName() { return siteName; }
    public void setSiteName(String siteName) { this.siteName = siteName; }

    public double getValue() { return value; }
    public void setValue(double value) { this.value = value; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public Long getWindowStart() { return windowStart; }
    public void setWindowStart(Long windowStart) { this.windowStart = windowStart; }

    public Long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(Long windowEnd) { this.windowEnd = windowEnd; }

    public Map<String, Object> getDetails() { return details; }
    public void setDetails(Map<String, Object> details) { this.details = details; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetricResult that = (MetricResult) o;
        return Double.compare(that.value, value) == 0
                && timestamp == that.timestamp
                && Objects.equals(metricName, that.metricName)
                && Objects.equals(adName, that.adName)
                && Objects.equals(siteName, that.siteName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metricName, adName, siteName, value, timestamp);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("MetricResult{metric='%s', ad='%s', site='%s', value=%.2f",
                metricName, adName, siteName, value));
        if (windowStart != null) {
            sb.append(String.format(", window=[%d-%d]", windowStart, windowEnd));
        }
        if (details != null && !details.isEmpty()) {
            sb.append(", details=").append(details);
        }
        sb.append('}');
        return sb.toString();
    }
}
