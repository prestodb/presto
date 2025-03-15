package com.facebook.presto.iceberg;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.RuntimeUnit;
import io.grpc.services.MetricReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;

import java.util.Objects;
//com.facebook.presto.tests.statistics;

public class RuntimeStatsMetricsReporter implements MetricsReporter {

    private final RuntimeStats runtimeStats;

    public RuntimeStatsMetricsReporter(RuntimeStats runtimeStats) {
        this.runtimeStats = runtimeStats;
    }

    @Override
    public void report(MetricsReport report) {

        if(!(report instanceof ScanReport)) {
            return;
        }

        // Cast report to scanReport instance
        ScanReport scanReport = (ScanReport)report;

        String table_name = scanReport.tableName();

        // TotalPlanning Duration Metric
        if(scanReport.scanMetrics().totalPlanningDuration() != null)
        {
            runtimeStats.addMetricValue(table_name + "-totalPlanningDuration", RuntimeUnit.NANO,
                    scanReport.scanMetrics().totalPlanningDuration().totalDuration().toNanos() );
        }


        // resultDataFiles Metric
        runtimeStats.addMetricValue(
                table_name + "-resultDataFiles",
                RuntimeUnit.NONE,
                scanReport.scanMetrics().resultDataFiles().value()
        );

        // resultDeleteFiles Metric
        runtimeStats.addMetricValue(
                table_name + "-resultDeleteFiles",
                RuntimeUnit.NONE,
                scanReport.scanMetrics().resultDeleteFiles().value()
        );


        // totalDataManifests Metric
        runtimeStats.addMetricValue(
                table_name + "-totalDataManifests",
                RuntimeUnit.NONE,
                scanReport.scanMetrics().totalDataManifests().value()
        );


        // totalDeleteManifests() Metric
        runtimeStats.addMetricValue(
                table_name + "-totalDeleteManifests",
                RuntimeUnit.NONE,
                scanReport.scanMetrics().totalDeleteManifests().value()
        );


        // scannedDataManifests() Metric
        runtimeStats.addMetricValue(
                table_name + "-scannedDataManifests",
                RuntimeUnit.NONE,
                scanReport.scanMetrics().scannedDataManifests().value()
        );


        // skippedDataManifests() Metric
        runtimeStats.addMetricValue(
                table_name + "-skippedDataManifests",
                RuntimeUnit.NONE,
                scanReport.scanMetrics().skippedDataManifests().value()
        );


        // totalFileSizeInBytes() -> RuntimeUnit.BYTES ?


        // totalDeleteFileSizeInBytes() -> RuntimeUnit.BYTES ?


        // skippedDataFiles() Metric


        // skippedDeleteFiles() Metric


        // scannedDeleteManifests() Metric


        // skippedDeleteManifests() Metric


        // indexedDeleteFiles() Metric


        // equalityDeleteFiles() Metric


        // positionalDeleteFiles() Metric






        // test this code by running Query on iceBerg connector
        // check webUi -> runtimestats
    }
}