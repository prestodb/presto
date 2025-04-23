/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.iceberg;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.RuntimeUnit;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;


/**
 * A MetricsReporter implementation for reporting
 * Iceberg scan metrics to Presto's RuntimeStats.
 */

public final class RuntimeStatsMetricsReporter implements MetricsReporter {


    /**
     * RuntimeStats variable used for storing scan metrics from Iceberg reports.
     */
    private final RuntimeStats runtimeStats;

    /**
     * Constructs a RuntimeStatsMetricsReporter.
     *
     * @param runtimeStat the RuntimeStats instance to report metrics to
     */
    public RuntimeStatsMetricsReporter(final RuntimeStats runtimeStat) {
        this.runtimeStats = runtimeStat;
    }

    /**
     * Helper method to construct the full metric name for a table scan.
     *
     * @param tableName the name of the table
     * @param metricName the name of the metric
     * @return the composed metric name in the format: table.scan.metric
     */
    private String tableScanString(final String tableName,
                                   final String metricName) {
        return tableName + ".scan." + metricName;
    }

    @Override
    public void report(final MetricsReport report) {

        if (!(report instanceof ScanReport)) {
            return;
        }

        // Cast report to scanReport instance
        ScanReport scanReport = (ScanReport) report;

        String tableName = scanReport.tableName();

        // TotalPlanning Duration Metric
        if (scanReport.scanMetrics().totalPlanningDuration() != null) {
            runtimeStats.addMetricValue(
                    tableScanString(tableName, "totalPlanningDuration"),
                    RuntimeUnit.NANO,
                    scanReport.scanMetrics().totalPlanningDuration()
                            .totalDuration().toNanos());
        }

        // resultDataFiles Metric
        if (scanReport.scanMetrics().resultDataFiles() != null) {
            runtimeStats.addMetricValue(
                    tableScanString(tableName, "resultDataFiles"),
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().resultDataFiles().value());
        }

        // resultDeleteFiles Metric
        if (scanReport.scanMetrics().resultDeleteFiles() != null) {
            runtimeStats.addMetricValue(
                    tableScanString(tableName, "resultDeleteFiles"),
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().resultDeleteFiles().value());
        }

        // totalDataManifests Metric
        if (scanReport.scanMetrics().totalDataManifests() != null) {
            runtimeStats.addMetricValue(
                    tableScanString(tableName, "totalDataManifests"),
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().totalDataManifests().value());
        }

        // totalDeleteManifests() Metric
        if (scanReport.scanMetrics().totalDeleteManifests() != null) {
            runtimeStats.addMetricValue(
                    tableScanString(tableName, "totalDeleteManifests"),
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().totalDeleteManifests().value());
        }

        // scannedDataManifests() Metric
        if (scanReport.scanMetrics().scannedDataManifests() != null) {
            runtimeStats.addMetricValue(
                    tableScanString(tableName, "scannedDataManifests"),
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().scannedDataManifests().value());
        }

        // skippedDataManifests() Metric
        if (scanReport.scanMetrics().skippedDataManifests() != null) {
            runtimeStats.addMetricValue(
                    tableScanString(tableName, "skippedDataManifests"),
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().skippedDataManifests().value());
        }

        // totalFileSizeInBytes() -> RuntimeUnit.BYTES ?
        if (scanReport.scanMetrics().totalFileSizeInBytes() != null) {
            runtimeStats.addMetricValue(
                    tableScanString(tableName, "totalFileSizeInBytes"),
                    RuntimeUnit.BYTE,
                    scanReport.scanMetrics().totalFileSizeInBytes()
                            .value());
        }

        // totalDeleteFileSizeInBytes() -> RuntimeUnit.BYTES ?
        if (scanReport.scanMetrics().totalDeleteFileSizeInBytes() != null) {
            runtimeStats.addMetricValue(
                    tableScanString(tableName, "totalDeleteFileSizeInBytes"),
                    RuntimeUnit.BYTE,
                    scanReport.scanMetrics().totalDeleteFileSizeInBytes()
                            .value());
        }

        // skippedDataFiles() Metric
        if (scanReport.scanMetrics().skippedDataFiles() != null) {
            runtimeStats.addMetricValue(
                    tableScanString(tableName, "skippedDataFiles"),
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().skippedDataFiles()
                            .value());
        }

        // skippedDeleteFiles() Metric
        if (scanReport.scanMetrics().skippedDeleteFiles() != null) {
            runtimeStats.addMetricValue(
                    tableScanString(tableName,  "skippedDeleteFiles"),
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().skippedDeleteFiles().value());
        }

        // scannedDeleteManifests() Metric
        if (scanReport.scanMetrics().scannedDeleteManifests() != null) {
            runtimeStats.addMetricValue(
                    tableScanString(tableName, "scannedDeleteManifests"),
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().scannedDeleteManifests().value());
        }

        // skippedDeleteManifests() Metric
        if (scanReport.scanMetrics().skippedDeleteManifests() != null) {
            runtimeStats.addMetricValue(
                    tableScanString(tableName, "skippedDeleteManifests"),
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().skippedDeleteManifests().value());
        }

        // indexedDeleteFiles() Metric
        if (scanReport.scanMetrics().indexedDeleteFiles() != null) {
            runtimeStats.addMetricValue(
                    tableScanString(tableName, "indexedDeleteFiles"),
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().indexedDeleteFiles().value());
        }

        // equalityDeleteFiles() Metric
        if (scanReport.scanMetrics().equalityDeleteFiles() != null) {
            runtimeStats.addMetricValue(
                    tableScanString(tableName,  "equalityDeleteFiles"),
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().equalityDeleteFiles().value());
        }

        // positionalDeleteFiles() Metric
        if (scanReport.scanMetrics().positionalDeleteFiles() != null) {
            runtimeStats.addMetricValue(
                    tableScanString(tableName, "positionalDeleteFiles"),
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().positionalDeleteFiles().value());
        }
    }
}
