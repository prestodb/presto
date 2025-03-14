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

        ScanReport scanReport = (ScanReport)report;

        //tablename-nameofmetrics
        //tablename-totalPlanningDuration

        //

        String table_name = scanReport.tableName();
        // check null
        if(scanReport.scanMetrics().totalPlanningDuration() != null && Objects.requireNonNull(scanReport.scanMetrics().totalPlanningDuration()).totalDuration() != null)
        {
            runtimeStats.addMetricValue(table_name + "-totalPlanningDuration", RuntimeUnit.NANO,
                    scanReport.scanMetrics().totalPlanningDuration().totalDuration().toNanos() );
        }

        // do rest for ScanReport

        // can test by run Query on iCeberg Connector
        // check webUi in runtimestats








    }


}