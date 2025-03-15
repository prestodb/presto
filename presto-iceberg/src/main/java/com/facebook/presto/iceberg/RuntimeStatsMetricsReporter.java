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
            runtimeStats.addMetricValue(
                    table_name + "-totalPlanningDuration",
                    RuntimeUnit.NANO,
                    scanReport.scanMetrics().totalPlanningDuration().totalDuration().toNanos()
            );
        }

        // resultDataFiles Metric
        if(scanReport.scanMetrics().resultDataFiles() != null)
        {
            runtimeStats.addMetricValue(
                    table_name + "-resultDataFiles",
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().resultDataFiles().value()
            );
        }

        // resultDeleteFiles Metric
        if(scanReport.scanMetrics().resultDeleteFiles() != null)
        {
            runtimeStats.addMetricValue(
                    table_name + "-resultDeleteFiles",
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().resultDeleteFiles().value()
            );
        }

        // totalDataManifests Metric
        if(scanReport.scanMetrics().totalDataManifests() != null)
        {
            runtimeStats.addMetricValue(
                    table_name + "-totalDataManifests",
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().totalDataManifests().value()
            );
        }



        // totalDeleteManifests() Metric
        if(scanReport.scanMetrics().totalDeleteManifests() != null)
        {
            runtimeStats.addMetricValue(
                    table_name + "-totalDeleteManifests",
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().totalDeleteManifests().value()
            );
        }


        // scannedDataManifests() Metric
        if(scanReport.scanMetrics().scannedDataManifests() != null)
        {
            runtimeStats.addMetricValue(
                    table_name + "-scannedDataManifests",
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().scannedDataManifests().value()
            );
        }



        // skippedDataManifests() Metric
        if( scanReport.scanMetrics().skippedDataManifests() != null)
        {
            runtimeStats.addMetricValue(
                    table_name + "-skippedDataManifests",
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().skippedDataManifests().value()
            );
        }



        // totalFileSizeInBytes() -> RuntimeUnit.BYTES ?
        if(scanReport.scanMetrics().totalFileSizeInBytes() != null)
        {
            runtimeStats.addMetricValue(
                    table_name + "-totalFileSizeInBytes",
                    RuntimeUnit.BYTE,
                    scanReport.scanMetrics().totalFileSizeInBytes().value()
            );
        }



        // totalDeleteFileSizeInBytes() -> RuntimeUnit.BYTES ?
        if(scanReport.scanMetrics().totalDeleteFileSizeInBytes() != null)
        {
            runtimeStats.addMetricValue(
                    table_name + "-totalDeleteFileSizeInBytes",
                    RuntimeUnit.BYTE,
                    scanReport.scanMetrics().totalDeleteFileSizeInBytes().value()
            );
        }


        // skippedDataFiles() Metric
        if(scanReport.scanMetrics().skippedDataFiles() != null)
        {
            runtimeStats.addMetricValue(
                    table_name + "-skippedDataFiles",
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().skippedDataFiles().value()
            );
        }



        // skippedDeleteFiles() Metric
        if(scanReport.scanMetrics().skippedDeleteFiles() != null)
        {
            runtimeStats.addMetricValue(
                    table_name + "-skippedDeleteFiles",
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().skippedDeleteFiles().value()
            );
        }



        // scannedDeleteManifests() Metric
        if(scanReport.scanMetrics().scannedDeleteManifests() != null)
        {
            runtimeStats.addMetricValue(
                    table_name + "-scannedDeleteManifests",
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().scannedDeleteManifests().value()
            );
        }


        // skippedDeleteManifests() Metric
        if(scanReport.scanMetrics().skippedDeleteManifests() != null)
        {
            runtimeStats.addMetricValue(
                    table_name + "-skippedDeleteManifests",
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().skippedDeleteManifests().value()
            );
        }



        // indexedDeleteFiles() Metric
        if(scanReport.scanMetrics().indexedDeleteFiles() != null)
        {
            runtimeStats.addMetricValue(
                    table_name + "-indexedDeleteFiles",
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().indexedDeleteFiles().value()
            );
        }



        // equalityDeleteFiles() Metric
        if(scanReport.scanMetrics().equalityDeleteFiles() != null)
        {
            runtimeStats.addMetricValue(
                    table_name + "-equalityDeleteFiles",
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().equalityDeleteFiles().value()
            );
        }


        // positionalDeleteFiles() Metric
        if(scanReport.scanMetrics().positionalDeleteFiles() != null)
        {
            runtimeStats.addMetricValue(
                    table_name + "-positionalDeleteFiles",
                    RuntimeUnit.NONE,
                    scanReport.scanMetrics().positionalDeleteFiles().value()
            );
        }


        // test this code by running Query on iceBerg connector
        // check webUi -> runtimestats

        // Next steps: Write tests too / Test w Query
    }
}