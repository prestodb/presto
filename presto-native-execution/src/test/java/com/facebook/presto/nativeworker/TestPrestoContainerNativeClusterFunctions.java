package com.facebook.presto.nativeworker;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class TestPrestoContainerNativeClusterFunctions extends AbstractTestQueryFramework {

    @Override
    protected ContainerQueryRunner createQueryRunner() throws Exception {
        // Default: native cluster with sidecar
        return new ContainerQueryRunner(1, true, true, false);
    }

    // ========== Native Cluster: With Sidecar Running ==========

    @Test
    public void testFunctionAnalysisWorks() {
        assertQuery("SELECT 1 + 1", "SELECT 2");
    }

    @Test
    public void testFailFunctionFails() {
        assertQueryFails("SELECT fail('boom')", "Function 'fail' not registered");
    }

    @Test
    public void testXFunctionSucceeds() {
        assertQuery("SELECT X(1)", "SELECT 42");
    }

    @Test
    public void testArraySortWorks() {
        assertQuery("SELECT array_sort(ARRAY [5, 20, null, 5, 3, 50])", "SELECT ARRAY[3, 5, 5, 20, 50, null]");
    }

    @Test
    public void testShowFunctionsNativeNamespace() {
        MaterializedResult result = computeActual("SHOW FUNCTIONS");
        for (MaterializedRow row : result) {
            String functionName = row.getField(0).toString();  // First column = function name
            System.out.println(functionName);
        }
        assertTrue(computeActual("SHOW FUNCTIONS").toString().contains("presto.native"));
    }

    // ========== Native Cluster: Without Sidecar Running ==========

    @Test
    public void testArraySortFailsWithoutSidecar() throws Exception {
        try (ContainerQueryRunner runner = new ContainerQueryRunner(1, true, false, false)) {
            runner.execute("SELECT array_sort(ARRAY[3, 1, 2])");
        }
        catch (Exception e) {
            String result = e.toString();
            assertTrue(result.contains("java.lang.UnsupportedOperationException"));
        }

    }

//     ========== Native Cluster: With Sidecar Starting Up ==========

    @Test
    public void testSidecarStartupTransition() throws Exception {
        try (ContainerQueryRunner runner = new ContainerQueryRunner(1, true, true, false)) {
//            try {
//                runner.execute("SELECT array_sort(ARRAY[3, 1, 2])");
//            } catch (Exception e) {
//                String result = e.toString();
//                assertTrue(result.contains("java.lang.UnsupportedOperationException"));
//            }

//            Thread.sleep(10000);

            // Step 3: should now succeed
            String result = runner.execute("SELECT (ARRAY[3, 1, 2]").toString().trim();
            String expected = runner.execute("SELECT ARRAY[1, 2, 3]").toString().trim();
            assertEquals(result, expected, "array_sort should succeed once sidecar is available");
        }
    }

}
