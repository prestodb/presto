import com.facebook.presto.Session;
import com.facebook.presto.common.RuntimeMetric;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.testing.assertions.Assert.assertTrue;

public class TestRuntimeStatsMetricsReporter
        extends AbstractTestQueryFramework
{
    @Test
    public void testRuntimeStatsMetricsReporter()
            throws Exception
    {
        // 1) Create (or reuse) an Iceberg catalog with the properties you need
        String catalogName = "ice_test_metrics_reporter";
        Map<String, String> catalogProperties = ImmutableMap.<String, String>builder()
                .putAll(getIcebergPropertiesFromBase()) // If you have a helper
                .build();
        getQueryRunner().createCatalog(catalogName, "iceberg", catalogProperties);

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalog(catalogName)
                .build();

        assertQuerySucceeds(session, "SELECT * FROM lineitem LIMIT 10");

        RuntimeStats runtimeStats = session.getRuntimeStats();

        // Check 'resultDataFiles' metric
        Optional<String> resultDataFilesMetricKey = runtimeStats.getMetrics().keySet().stream()
                .filter(name -> name.contains("lineitem-resultDataFiles"))
                .findFirst();

        assertTrue(resultDataFilesMetricKey.isPresent(),
                "Expected 'lineitem-resultDataFiles' metric to be reported for table lineitem");
        RuntimeMetric resultDataFilesMetric = runtimeStats.getMetric(resultDataFilesMetricKey.get());
        assertTrue(
                resultDataFilesMetric.getSum() > 0,
                "Expected the sum of the 'lineitem-resultDataFiles' metric to be > 0"
        );

        // Check 'totalPlanningDuration' metric
        Optional<String> totalPlanningDurationKey = runtimeStats.getMetrics().keySet().stream()
                .filter(name -> name.contains("lineitem-totalPlanningDuration"))
                .findFirst();

        // totalPlanningDuration is recorded in NANO units by the reporter
        if (totalPlanningDurationKey.isPresent()) {
            RuntimeMetric totalPlanningDurationMetric = runtimeStats.getMetric(totalPlanningDurationKey.get());
            assertTrue(
                    totalPlanningDurationMetric.getSum() > 0,
                    "Expected totalPlanningDuration to be > 0 nanos"
            );
        } else {
            // If no planning was recorded, you can either assert or skip
            System.out.println("No totalPlanningDuration metric was reported—possible that no planning was needed.");
        }

        // Check 'totalFileSizeInBytes' metric
        Optional<String> totalFileSizeInBytesKey = runtimeStats.getMetrics().keySet().stream()
                .filter(name -> name.contains("lineitem-totalFileSizeInBytes"))
                .findFirst();

        // totalFileSizeInBytes is recorded in BYTE units by the reporter
        if (totalFileSizeInBytesKey.isPresent()) {
            RuntimeMetric totalFileSizeInBytesMetric = runtimeStats.getMetric(totalFileSizeInBytesKey.get());
            assertTrue(
                    totalFileSizeInBytesMetric.getSum() > 0,
                    "Expected totalFileSizeInBytes to be > 0 bytes"
            );
        } else {
            System.out.println("No totalFileSizeInBytes metric was reported—maybe the table is empty or fully pruned.");
        }
    }

    private Map<String, String> getIcebergPropertiesFromBase()
    {
        // Adapt as appropriate for your codebase; if you don't have a helper, just return an empty map or
        // the properties you need. The snippet below is a placeholder for demonstration:
        return getIcebergQueryRunner().getIcebergCatalogs().get("iceberg");
    }
}
