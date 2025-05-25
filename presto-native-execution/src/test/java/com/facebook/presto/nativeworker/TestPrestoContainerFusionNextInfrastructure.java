package com.facebook.presto.nativeworker;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class TestPrestoContainerFusionNextInfrastructure extends AbstractTestQueryFramework {

    @Override
    protected ContainerQueryRunner createQueryRunner() throws Exception {
        // Default: native cluster with sidecar
        return new ContainerQueryRunner(1, true, true, false);
    }

    @Test
    public void testInfrastructureNativeClusterWithSidecar() {
//        assertQueryFails("SELECT fail('forced failure')", "forced failure");

//        assertQuery("SELECT array_sort(ARRAY [5, 20, null, 5, 3, 50])", "SELECT ARRAY[3, 5, 5, 20, 50, null]");
        computeActual("select array_sort(array[row('apples', 23), row('bananas', 12), row('grapes', 44)], x -> x[2])");
        // need to discuss what should be checked under show functions ie function count ?
        assertTrue(computeActual("SHOW FUNCTIONS").toString().contains("presto.native"));
    }

    @Test
    public void testInfrastructureNativeClusterWithoutSidecar() {
        try(QueryRunner queryRunner = new ContainerQueryRunner(1, true, false, false)) {
            assertQueryFailsWithCustomQueryRunner(queryRunner, "SELECT fail('forced failure')", "presto.default.fail\\(forced failure:VARCHAR\\)",true);

            assertQueryFailsWithCustomQueryRunner(queryRunner, "select array_sort(array[row('apples', 23), row('bananas', 12), row('grapes', 44)], x -> x[2])", "Expected a lambda that takes 2 argument\\(s\\) but got 1", true);
            // need to discuss what should be checked under show functions ie function count ?
//            assertTrue(computeActualWithAlternateRunner(queryRunner,"SHOW FUNCTIONS").toString().contains("presto.default"));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testInfrastructureNativeClusterDelayedSidecar() {
        try(QueryRunner queryRunner = new ContainerQueryRunner(1, true, true, false)) {
            try{
                assertQueryFailsWithCustomQueryRunner(queryRunner, "select array_sort(array[row('apples', 23), row('bananas', 12), row('grapes', 44)], x -> x[2])", "Function native.default.array_sort not registered", true);
            }
            catch (Exception e) {
                String result = e.toString();
                assertTrue(result.contains("java.lang.UnsupportedOperationException"));
            }
//            Thread.sleep(10000);
//            assertQueryWithAlternateQueryRunner(queryRunner, "select array_sort(array[row('apples', 23), row('bananas', 12), row('grapes', 44)], x -> x[2])", );
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
// This case should fail due to invalid conf but still passing this means there is some issue with the configuration, further investigation needed
    @Test
    public void testInfrastructureJavaClusterWithSidecar() {
        try(QueryRunner queryRunner = new ContainerQueryRunner(1, false, true, false)) {
            System.out.println(computeActualWithAlternateRunner(queryRunner, "SELECT array_sort(ARRAY [5, 20, null, 5, 3, 50])").toString());
            assertQueryFailsWithCustomQueryRunner(queryRunner, "SELECT array_sort(ARRAY [5, 20, null, 5, 3, 50])", "InvalidConfiguration", true);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testInfrastructureJavaClusterWithoutSidecar() {
        try(QueryRunner queryRunner = new ContainerQueryRunner(1, false, false, false)) {
//            assertQueryFailsWithCustomQueryRunner(queryRunner, "SELECT fail('forced failure')", "=", true);
            assertQueryFailsWithCustomQueryRunner(queryRunner, "select array_sort(array[row('apples', 23), row('bananas', 12), row('grapes', 44)], x -> x[2])", "Expected a lambda that takes 2 argument\\(s\\) but got 1", true);
            System.out.println(computeActualWithAlternateRunner(queryRunner,"SHOW SESSION").toString());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}