package com.facebook.presto.iceberg;

import com.facebook.presto.common.RuntimeStats;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;

public class RuntimeStatsMetricsReporter implements MetricsReporter {

    private final RuntimeStats runtimeStats;

    public RuntimeStatsMetricsReporter(RuntimeStats runtimeStats) {
        this.runtimeStats = runtimeStats;
    }

    public void reportMetrics(MetricsReport report) {

        // for (Metric metric in report) {
        //  runtimeStats.update(...)
        // }
    }

    @Override
    public void report(MetricsReport metricsReport) {

    }
}