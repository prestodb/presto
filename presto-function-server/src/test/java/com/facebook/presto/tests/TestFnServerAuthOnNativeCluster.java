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
package com.facebook.presto.tests;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.utils.FnServerAuthTestUtils;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * Tests mTLS + JWT communication between:
 * - Java Coordinator
 * - C++ (Native) Worker
 * - Function Server
 *
 * This tests the most secure configuration with both certificate-based
 * authentication (mTLS) and token-based authentication (JWT) in a hybrid
 * Java/Native cluster environment.
 */
public class TestFnServerAuthOnNativeCluster
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestFnServerAuthOnNativeCluster.class);

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return FnServerAuthTestUtils.createNativeRunnerWithValidHttpsFnServer();
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
     * Tests secure authentication (mTLS + JWT) between Native Worker and Function-Server
     * with table data processing.
     */
    @Test
    public void testMtlsJwtBetweenNativeWorkerAndFnServer()
    {
        log.info("TEST: Remote function execution with mTLS + JWT (native worker→function server)");
        MaterializedResult result = computeActual(
                "SELECT rest.default.sqrt(n_nationkey) FROM nation LIMIT 5");
        assertEquals(result.getRowCount(), 5);
        log.info("Table query succeeded: %d rows processed", result.getRowCount());
    }

    /**
     * Test mTLS-only communication between Coordinator, Native Worker, and Function-Server.
     */
    @Test
    public void testMtlsOnlyCommunicationToFnServer()
    {
        log.info("TEST: mTLS-only communication between Coordinator, Native Worker, and Function-Server");
        try {
            try (QueryRunner mtlsRunner = FnServerAuthTestUtils
                    .createNativeRunnerWithOnlyMtls()) {
                MaterializedResult result1 = mtlsRunner.execute(getSession(),
                        "SELECT rest.default.ceil(3.14)");
                assertEquals(result1.getOnlyValue(), 4.0f);
                log.info("Simple function call succeeded: ceil(3.14) = %f", result1.getOnlyValue());
            }
        }
        catch (Exception e) {
            log.error("Error message: %s", e.getMessage());
            fail("mTLS-only communication should succeed", e);
        }
    }

    /**
     * Test mTLS + JWT communication when Function-Server has invalid certificate.
     */
    @Test(expectedExceptions = Exception.class)
    public void testMtlsJwtCommunicationWhenFnServerHasInvalidCert() throws Exception
    {
        log.info("TEST: mTLS + JWT when Function Server has Invalid Certificate");
        try {
            try (QueryRunner invalidRunner = FnServerAuthTestUtils
                    .createNativeRunnerWithInvalidCertInFnServer()) {
                MaterializedResult result = invalidRunner.execute(getSession(),
                        "SELECT rest.default.mod(o_orderkey, 10) FROM orders LIMIT 5");
                assertEquals(result.getRowCount(), 5);
                log.info("Table query succeeded: %d rows processed", result.getRowCount());
            }
            fail("Query should have failed - coordinator/native worker should reject invalid certificate");
        }
        catch (Exception e) {
            log.info("Error message: %s", e.getMessage());
            throw e;
        }
    }

    /**
     * Test JWT communication with wrong shared secret in function-server configuration.
     */
    @Test(expectedExceptions = Exception.class)
    public void testJwtCommunicationWithWrongSecretInFnServer()
            throws Exception
    {
        log.info("TEST: JWT communication with wrong shared secret in function-server");
        try {
            try (QueryRunner invalidRunner = FnServerAuthTestUtils
                    .createNativeRunnerWithWrongJwtSecretOnFnServer()) {
                invalidRunner.execute(getSession(),
                        "SELECT rest.default.sign(o_totalprice) FROM orders LIMIT 5");
            }
            fail("Query should have failed - coordinator/native worker should reject invalid JWT secret");
        }
        catch (Exception e) {
            log.info("Error message: %s", e.getMessage());
            throw e;
        }
    }

    /**
     * Test JWT communication when JWT properties are removed from function-server configuration.
     */
    @Test(expectedExceptions = Exception.class)
    public void testJwtCommunicationWhenPropsRemovedOnFnServer()
            throws Exception
    {
        log.info("TEST: JWT communication when JWT properties removed from function-server");
        try {
            try (QueryRunner invalidRunner = FnServerAuthTestUtils
                    .createNativeRunnerWithNoJwtOnFnServer()) {
                invalidRunner.execute(getSession(),
                        "SELECT rest.default.round(l_extendedprice, 2) " +
                                "FROM lineitem LIMIT 10");
            }
            fail("Query should have failed - JWT validation should be enforced");
        }
        catch (Exception e) {
            log.info("Expected failure occurred: %s", e.getClass().getSimpleName());
            log.info("Error message: %s", e.getMessage());
            throw e;
        }
    }
}
