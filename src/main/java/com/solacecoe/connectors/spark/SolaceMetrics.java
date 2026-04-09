package com.solacecoe.connectors.spark;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SettableGauge;
import org.apache.spark.metrics.source.Source;

import java.util.Map;

public class SolaceMetrics implements Source {
    private final MetricRegistry metricRegistry;
    public SolaceMetrics() {
        this.metricRegistry = new MetricRegistry();
    }
    @Override
    public String sourceName() {
        return "solace-metrics";
    }

    @Override
    public MetricRegistry metricRegistry() {
        return this.metricRegistry;
    }

    public SettableGauge<Map<String, String>> sessionStats(String metricName) {
        return this.metricRegistry().gauge(MetricRegistry.name(metricName));
    }
}
