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
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Abstract base class for Function Server authentication tests.
 */
public abstract class AbstractTestFnServerAuth
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(AbstractTestFnServerAuth.class);

    /**
     * Creates a runner where Function Server, Coordinator, and Worker all use
     * mTLS only (no JWT).
     */
    protected abstract QueryRunner createRunnerWithOnlyMtls() throws Exception;

    /**
     * Creates a runner where the Function Server has an invalid (untrusted) certificate.
     */
    protected abstract QueryRunner createRunnerWithInvalidCertInFnServer() throws Exception;

    /**
     * Creates a runner where the Function Server is configured with the wrong JWT shared secret.
     */
    protected abstract QueryRunner createRunnerWithWrongJwtSecretOnFnServer() throws Exception;

    /**
     * Creates a runner where the Function Server has no JWT properties at all.
     */
    protected abstract QueryRunner createRunnerWithNoJwtOnFnServer() throws Exception;

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
     * Tests mTLS + JWT communication between Worker and Function-Server with table data.
     * Uses the primary QueryRunner created by {@link #createQueryRunner()}.
     */
    @Test
    public void testMtlsJwtWithWorkerAndFnServer()
    {
        log.info("TEST: Remote function execution with mTLS + JWT (worker->function server)");
        MaterializedResult result = computeActual(
                "SELECT rest.default.sqrt(n_nationkey) FROM nation LIMIT 5");
        assertEquals(result.getRowCount(), 5);
        log.info("Table query succeeded: %d rows processed", result.getRowCount());
    }

    /**
     * Tests mTLS-only communication between Coordinator, Worker, and Function-Server.
     */
    @Test
    public void testMtlsOnlyCommunicationToFnServer()
            throws Exception
    {
        log.info("TEST: mTLS-only communication between Coordinator, Worker, and Function-Server");

        try (QueryRunner mtlsRunner = createRunnerWithOnlyMtls()) {
            MaterializedResult result = mtlsRunner.execute(getSession(), "SELECT rest.default.ceil(3.14)");
            assertEquals(result.getOnlyValue(), 4.0f);
            log.info("Simple function call succeeded: ceil(3.14) = %f", result.getOnlyValue());
        }
    }

    /**
     * Tests that a Function-Server with an invalid certificate is rejected.
     */
    @Test(expectedExceptions = Exception.class)
    public void testMtlsCommunicationWhenFnServerHasInvalidCert()
            throws Exception
    {
        log.info("TEST: mTLS + JWT when Function Server has Invalid Keystore");

        try (QueryRunner invalidRunner = createRunnerWithInvalidCertInFnServer()) {
            invalidRunner.execute(getSession(), "SELECT rest.default.mod(o_orderkey, 10) FROM orders LIMIT 5");
            fail("Query should have failed - coordinator/worker should reject invalid certificate");
        }
        catch (Exception e) {
            log.info("Expected failure occurred: %s", e.getClass().getSimpleName());
            log.info("Error message: %s", e.getMessage());
            String message = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
            assertTrue(
                    message.contains("ssl") || message.contains("certificate") || message.contains("handshake"),
                    "Expected an SSL/certificate failure but got: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Tests that a Function-Server configured with the wrong JWT secret is rejected.
     */
    @Test(expectedExceptions = Exception.class)
    public void testJwtCommunicationWithWrongSecretInFnServer()
            throws Exception
    {
        log.info("TEST: JWT communication with wrong shared secret in function-server");

        try (QueryRunner invalidRunner = createRunnerWithWrongJwtSecretOnFnServer()) {
            invalidRunner.execute(getSession(), "SELECT rest.default.sign(o_totalprice) FROM orders LIMIT 5");
            fail("Query should have failed - coordinator/worker should reject invalid jwt secret");
        }
        catch (Exception e) {
            log.info("Expected failure occurred: %s", e.getClass().getSimpleName());
            log.info("Error message: %s", e.getMessage());
            String message = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
            assertTrue(
                    message.contains("401") || message.contains("unauthorized") || message.contains("jwt"),
                    "Expected a JWT authentication failure but got: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Tests that a Function-Server with no JWT configuration is rejected.
     */
    @Test(expectedExceptions = Exception.class)
    public void testJwtCommunicationWhenPropsRemovedOnFnServer()
            throws Exception
    {
        log.info("TEST: JWT communication by removing the JWT properties from function-server configuration");

        try (QueryRunner invalidRunner = createRunnerWithNoJwtOnFnServer()) {
            invalidRunner.execute(getSession(), "SELECT rest.default.round(l_extendedprice, 2) FROM lineitem LIMIT 10");
            fail("Query should have failed with no JWT related props on function-server");
        }
        catch (Exception e) {
            log.info("Expected failure occurred: %s", e.getClass().getSimpleName());
            log.info("Error message: %s", e.getMessage());
            String message = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
            assertTrue(
                    message.contains("401") || message.contains("unauthorized") || message.contains("jwt"),
                    "Expected a JWT authentication failure but got: " + e.getMessage());
            throw e;
        }
    }
}
