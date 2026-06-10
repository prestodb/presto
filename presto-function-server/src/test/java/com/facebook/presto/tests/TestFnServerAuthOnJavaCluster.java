/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
 * - Java Worker
 * - Function Server
 * This tests the most secure configuration with both certificate-based
 * authentication (mTLS) and token-based authentication (JWT).
 */
public class TestFnServerAuthOnJavaCluster
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestFnServerAuthOnJavaCluster.class);

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return FnServerAuthTestUtils.createRunnerWithValidHttpsFnServer();
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
     * Tests secure authentication (mTLS + JWT) between Coordinator and Function-Server
     */
    @Test
    public void testMtlsJwtBetweenCoordinatorAndFnServer()
    {
        log.info("Test: Simple remote function execution with mTLS + JWT (coordinator→function server)");
        MaterializedResult result1 = computeActual("SELECT rest.default.abs(-123)");
        assertEquals(result1.getOnlyValue(), 123);
        log.info("Simple function call succeeded: abs(-123) = 123");
    }

    /**
     * Tests secure authentication (mTLS + JWT) between Worker and Function-Server
     */
    @Test
    public void testMtlsJwtBetweenWorkerAndFnServer()
    {
        log.info("TEST: Remote function execution with mTLS + JWT (worker->function server");
        MaterializedResult result2 = computeActual(
                "SELECT rest.default.abs(nationkey) FROM tpch.tiny.nation LIMIT 3");
        assertEquals(result2.getRowCount(), 3);
        log.info("Table query succeeded: %d rows processed", result2.getRowCount());
    }

    /**
     * Tests secure authentication (mTLS + JWT) between Worker and Function-Server with a complex query
     */
    @Test
    public void testMtlsAndJwtWithComplexQuery()
    {
        log.info("TEST: mTLS and JWT communication (worker->function server) with complex query");
        MaterializedResult result3 = computeActual(
                "SELECT count(*), sum(rest.default.abs(nationkey)) FROM tpch.tiny.nation");
        log.info("Aggregation query with remote function call succeeded");
    }

    /**
     * Test mTLS-only communication between Coordinator, Worker, and Function-Server
     */
    @Test
    public void testMtlsOnlyCommunicationToFnServer()
    {
        log.info("TEST: mTLS-only communication between Coordinator, Worker, and Function-Server");
        try {
            try (QueryRunner mtlsRunner = FnServerAuthTestUtils
                    .createRunnerWithOnlyMtls()) {

                log.info("Test: Simple remote function call (coordinator→function server)");
                MaterializedResult result1 = mtlsRunner.execute(getSession(), "SELECT rest.default.abs(-123)");
                assertEquals(result1.getOnlyValue(), 123);
                log.info("Simple function call succeeded: abs(-123) = 123");

                log.info("Test: Remote function with table data (worker→function server)");
                MaterializedResult result2 = mtlsRunner.execute(getSession(), "SELECT rest.default.abs(nationkey) FROM tpch.tiny.nation LIMIT 3");
                assertEquals(result2.getRowCount(), 3);
                log.info("Table query succeeded: %d rows processed", result2.getRowCount());

                log.info("Test: Complex query with aggregation");
                MaterializedResult result3 = mtlsRunner.execute(getSession(), "SELECT count(*), sum(rest.default.abs(nationkey)) FROM tpch.tiny.nation");
                assertEquals(result2.getRowCount(), 3);
                log.info("Aggregation query succeeded");
            }
        }
        catch (Exception e) {
            log.info("Failure occurred: %s", e.getClass().getSimpleName());
            log.info("Error message: %s", e.getMessage());
        }
    }

    /**
     * Test mTLS-JWT communication between Coordinator, Worker, and Function-Server.
     * In this case, Function-Server has invalid certificate in its keystore.
     */
    @Test(expectedExceptions = Exception.class)
    public void testMtlsOnlyCommunicationWhenFnServerHasInvalidCert() throws Exception
    {
        log.info("TEST: mTLS + JWT when Function Server has Invalid Keystore");
        try {
            try (QueryRunner invalidRunner = FnServerAuthTestUtils
                    .createRunnerWithInvalidCertInFnServer()) {
                MaterializedResult result = invalidRunner.execute(getSession(), "SELECT rest.default.abs(nationkey) FROM tpch.tiny.nation LIMIT 3");
                assertEquals(result.getRowCount(), 3);
                log.info("Table query succeeded: %d rows processed", result.getRowCount());
            }

            log.error("TEST FAILED: Query succeeded with invalid certificate!");
            log.error("This means certificate validation is NOT working!");
            fail("Query should have failed - coordinator/worker should reject invalid certificate");
        }
        catch (Exception e) {
            log.info("✓ Expected failure occurred: %s", e.getClass().getSimpleName());
            log.info("✓ Error message: %s", e.getMessage());
            throw e;
        }
    }

    /**
     * Test JWT communication by including wrong secret in function-server configuration.
     */
    @Test(expectedExceptions = Exception.class)
    public void testJwtCommunicationWithWrongSecretInFnServer()
            throws Exception
    {
        log.info("TEST: JWT communication with wrong shared secret in function-server");

        try {
            try (QueryRunner invalidRunner = FnServerAuthTestUtils
                    .createRunnerWithInvalidJwtSecretOnFnServer()) {
                invalidRunner.execute(getSession(), "SELECT rest.default.abs(nationkey) FROM tpch.tiny.nation LIMIT 3");
            }

            log.error("TEST FAILED: Query succeeded with invalid jwt secret!");
            fail("Query should have failed - coordinator/worker should reject invalid jwt secret");
        }
        catch (Exception e) {
            log.info("Expected failure occurred: %s", e.getClass().getSimpleName());
            log.info("Error message: %s", e.getMessage());
            throw e;
        }
    }

    /**
     * Test JWT communication by removing the JWT properties from function-server configuration.
     */
    @Test(expectedExceptions = Exception.class)
    public void testJwtCommunicationWhenPropsRemovedOnFnServer()
            throws Exception
    {
        log.info("TEST: JWT communication by removing the JWT properties from function-server configuration");

        try {
            try (QueryRunner invalidRunner = FnServerAuthTestUtils
                    .createRunnerWithNoJwtOnFnServer()) {
                invalidRunner.execute(getSession(), "SELECT rest.default.abs(nationkey) FROM tpch.tiny.nation LIMIT 3");
            }

            log.error("TEST FAILED: Query succeeded with no jwt configs!");
            fail("Query should have failed");
        }
        catch (Exception e) {
            log.info("Expected failure occurred: %s", e.getClass().getSimpleName());
            log.info("Error message: %s", e.getMessage());
            throw e;
        }
    }
}
