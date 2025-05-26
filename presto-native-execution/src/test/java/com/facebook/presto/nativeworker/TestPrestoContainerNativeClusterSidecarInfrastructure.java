package com.facebook.presto.nativeworker;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

public class TestPrestoContainerNativeClusterSidecarInfrastructure
        extends AbstractTestQueryFramework
{
    @Override
    protected ContainerNativeQueryRunnerWithSidecar createQueryRunner()
            throws Exception {
        // Default: native cluster with sidecar
        return new ContainerNativeQueryRunnerWithSidecar();
    }

    @Test
    public void Test1(){
        assertQuery("SELECT array_sort(ARRAY [5, 20, null, 5, 3, 50])");
    }

//    public void Test2(){
//
//    }
}
