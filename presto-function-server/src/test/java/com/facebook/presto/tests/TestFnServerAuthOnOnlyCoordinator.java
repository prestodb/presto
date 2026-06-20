package com.facebook.presto.tests;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.utils.FnServerAuthTestUtils;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestFnServerAuthOnOnlyCoordinator
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestFnServerAuthOnOnlyCoordinator.class);

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return FnServerAuthTestUtils.createCoordinatorOnlyRunnerWithMtlsAndJwt();
    }

    @Override
    protected Session getSession()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty("remote_functions_enabled", "true")
                .build();
    }

    /**
     * Tests secure authentication (mTLS + JWT) between Coordinator and Function-Server ONLY.
     * Uses coordinator-only query runner to ensure queries execute on coordinator, not workers.
     */
    @Test
    public void testMtlsJwtBetweenCoordinatorAndFnServer()
    {
        MaterializedResult result = computeActual(
                "SELECT rest.default.abs(-123)");
        assertEquals(result.getOnlyValue(), 123);
        log.info("Coordinator→Function-Server test passed with result value %d", result.getOnlyValue());
    }
}
