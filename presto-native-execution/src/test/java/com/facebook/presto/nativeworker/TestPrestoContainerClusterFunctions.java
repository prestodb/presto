package com.facebook.presto.nativeworker;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class TestPrestoContainerClusterFunctions extends AbstractTestQueryFramework {

    @Override
    protected ContainerQueryRunner createQueryRunner() throws Exception {
        // Default: native cluster with sidecar
        return new ContainerQueryRunner(1, true, true, false);
    }

    @Test
    public void testInfrastructureNativeClusterWithSidecar() {
        assertQueryFails("SELECT fail('forced failure')", "forced failure");

        assertQuery("SELECT array_sort(ARRAY [5, 20, null, 5, 3, 50])", "SELECT ARRAY[3, 5, 5, 20, 50, null]");

        MaterializedResult result = computeActual("SHOW FUNCTIONS");
        for (MaterializedRow row : result) {
            String functionName = row.getField(0).toString();  // First column = function name
            System.out.println(functionName);
        }
        assertTrue(computeActual("SHOW FUNCTIONS").toString().contains("presto.native"));
    }

    @Test
    public void testInfrastructureNativeClusterWithoutSidecar() {
        try(QueryRunner queryRunner = new ContainerQueryRunner(1, true, false, false)) {
//            assertQueryWithAlternateQueryRunner(queryRunner, "SELECT fail('forced failure')", "forced failure");
            try{
                assertQueryWithAlternateQueryRunner(queryRunner,"SELECT array_sort(ARRAY [5, 20, null, 5, 3, 50])", "SELECT ARRAY[3, 5, 5, 20, 50, null]");
            }
            catch (Exception e) {
                String result = e.toString();
                assertTrue(result.contains("java.lang.UnsupportedOperationException"));
            }
            assertTrue(computeActualWithAlternateRunner(queryRunner,"SHOW FUNCTIONS").toString().contains("presto.native"));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testInfrastructureNativeClusterDelayedSidecar() {
        try(QueryRunner queryRunner = new ContainerQueryRunner(1, true, true, true)) {
            try{
                assertTrue(computeActualWithAlternateRunner(queryRunner, "SELECT array_sort(ARRAY [5, 20, null, 5, 3, 50])").toString().contains("UnsupportedOperationException"));
            }
            catch (Exception e) {
                String result = e.toString();
                assertTrue(result.contains("java.lang.UnsupportedOperationException"));
            }
            Thread.sleep(10000);
            assertQueryWithAlternateQueryRunner(queryRunner, "SELECT array_sort(ARRAY [5, 20, null, 5, 3, 50])", "SELECT ARRAY[3, 5, 5, 20, 50, null]");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testInfrastructureJavaClusterWithSidecar() {
        try(QueryRunner queryRunner = new ContainerQueryRunner(1, false, true, false)) {
            try{
                assertQueryWithAlternateQueryRunner(queryRunner, "SELECT array_sort(ARRAY [5, 20, null, 5, 3, 50])", "SELECT ARRAY[3, 5, 5, 20, 50, null]");
            }
            catch (Exception e) {
                String result = e.toString();
                assertTrue(result.contains("InvalidConfiguration"));
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testInfrastructureJavaClusterWithoutSidecar() {
        try(QueryRunner queryRunner = new ContainerQueryRunner(1, false, false, false)) {
            assertQueryWithAlternateQueryRunner(queryRunner, "SELECT fail('forced failure')", "forced failure");

            assertQueryFailsWithAlternateQueryRunner(queryRunner, "SELECT array_sort(ARRAY [5, 20, null, 5, 3, 50])", "SELECT ARRAY[3, 5, 5, 20, 50, null]");

            assertTrue(computeActualWithAlternateRunner(queryRunner,"SHOW FUNCTIONS").toString().contains("presto.default"));
            System.out.println(computeActualWithAlternateRunner(queryRunner,"SHOW SESSION").toString());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}