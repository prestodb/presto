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
import static org.testng.Assert.assertTrue;
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
     * Tests secure authentication (mTLS + JWT) between Coordinator and Function-Server
     * with a simple scalar function call.
     */
    @Test
    public void testMtlsJwtBetweenCoordinatorAndFnServer()
    {
        log.info("TEST: Simple remote function execution with mTLS + JWT (coordinator→function server)");
        MaterializedResult result = computeActual("SELECT rest.default.power(2, 3)");
        assertEquals(result.getOnlyValue(), 8.0);
        log.info("Simple function call succeeded: power(2, 3) = 8.0");
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
                "SELECT rest.default.sqrt(regionkey) FROM tpch.tiny.region");
        assertEquals(result.getRowCount(), 5);
        log.info("Table query succeeded: %d rows processed", result.getRowCount());
    }

    /**
     * Tests secure authentication (mTLS + JWT) with complex aggregation query.
     */
    @Test
    public void testMtlsAndJwtWithComplexAggregation()
    {
        log.info("TEST: mTLS and JWT communication with complex aggregation query");
        MaterializedResult result = computeActual(
                "SELECT regionkey, count(*), avg(rest.default.abs(nationkey)) " +
                        "FROM tpch.tiny.nation " +
                        "GROUP BY regionkey " +
                        "ORDER BY regionkey");
        assertEquals(result.getRowCount(), 5);
        log.info("Aggregation query with remote function call succeeded: %d regions", result.getRowCount());
    }

    /**
     * Tests mTLS + JWT with string manipulation functions.
     */
    @Test
    public void testMtlsJwtWithStringFunctions()
    {
        log.info("TEST: mTLS + JWT with string manipulation functions");
        MaterializedResult result = computeActual(
                "SELECT rest.default.length(name) as name_length " +
                        "FROM tpch.tiny.nation " +
                        "WHERE nationkey < 5");
        assertEquals(result.getRowCount(), 5);
        log.info("String function query succeeded: %d rows processed", result.getRowCount());
    }

    /**
     * Tests mTLS + JWT with JOIN operations involving remote functions.
     */
    @Test
    public void testMtlsJwtWithJoinAndRemoteFunctions()
    {
        log.info("TEST: mTLS + JWT with JOIN operations and remote functions");
        MaterializedResult result = computeActual(
                "SELECT n.name, r.name, rest.default.abs(n.nationkey) " +
                        "FROM tpch.tiny.nation n " +
                        "JOIN tpch.tiny.region r ON n.regionkey = r.regionkey " +
                        "WHERE n.nationkey < 10");
        assertTrue(result.getRowCount() > 0);
        log.info("JOIN query with remote functions succeeded: %d rows", result.getRowCount());
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

                log.info("Test: Simple remote function call (coordinator→function server)");
                MaterializedResult result1 = mtlsRunner.execute(getSession(),
                        "SELECT rest.default.ceil(3.14)");
                assertEquals(result1.getOnlyValue(), 4.0);
                log.info("Simple function call succeeded: ceil(3.14) = 4.0");

                log.info("Test: Remote function with table data (native worker→function server)");
                MaterializedResult result2 = mtlsRunner.execute(getSession(),
                        "SELECT rest.default.floor(acctbal) FROM tpch.tiny.customer LIMIT 5");
                assertEquals(result2.getRowCount(), 5);
                log.info("Table query succeeded: %d rows processed", result2.getRowCount());

                log.info("Test: Complex query with aggregation and remote functions");
                MaterializedResult result3 = mtlsRunner.execute(getSession(),
                        "SELECT mktsegment, count(*), sum(rest.default.abs(custkey)) " +
                                "FROM tpch.tiny.customer " +
                                "GROUP BY mktsegment");
                assertTrue(result3.getRowCount() > 0);
                log.info("Aggregation query succeeded: %d segments", result3.getRowCount());
            }
        }
        catch (Exception e) {
            log.error("Unexpected failure occurred: %s", e.getClass().getSimpleName());
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
                        "SELECT rest.default.mod(orderkey, 10) FROM tpch.tiny.orders LIMIT 5");
                assertEquals(result.getRowCount(), 5);
                log.info("Table query succeeded: %d rows processed", result.getRowCount());
            }

            log.error("TEST FAILED: Query succeeded with invalid certificate!");
            log.error("This means certificate validation is NOT working!");
            fail("Query should have failed - coordinator/native worker should reject invalid certificate");
        }
        catch (Exception e) {
            log.info("✓ Expected failure occurred: %s", e.getClass().getSimpleName());
            log.info("✓ Error message: %s", e.getMessage());
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
                    .createNativeRunnerWithInvalidJwtSecretOnFnServer()) {
                invalidRunner.execute(getSession(),
                        "SELECT rest.default.sign(totalprice) FROM tpch.tiny.orders LIMIT 5");
            }

            log.error("TEST FAILED: Query succeeded with invalid JWT secret!");
            fail("Query should have failed - coordinator/native worker should reject invalid JWT secret");
        }
        catch (Exception e) {
            log.info("✓ Expected failure occurred: %s", e.getClass().getSimpleName());
            log.info("✓ Error message: %s", e.getMessage());
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
                        "SELECT rest.default.round(extendedprice, 2) " +
                                "FROM tpch.tiny.lineitem LIMIT 10");
            }

            log.error("TEST FAILED: Query succeeded without JWT configuration!");
            fail("Query should have failed - JWT validation should be enforced");
        }
        catch (Exception e) {
            log.info("✓ Expected failure occurred: %s", e.getClass().getSimpleName());
            log.info("✓ Error message: %s", e.getMessage());
            throw e;
        }
    }

    /**
     * Test mTLS + JWT with subquery and remote functions.
     */
    @Test
    public void testMtlsJwtWithSubqueryAndRemoteFunctions()
    {
        log.info("TEST: mTLS + JWT with subquery and remote functions");
        MaterializedResult result = computeActual(
                "SELECT name, rest.default.abs(nationkey) as abs_key " +
                        "FROM tpch.tiny.nation " +
                        "WHERE nationkey IN (SELECT nationkey FROM tpch.tiny.customer WHERE custkey < 100) " +
                        "ORDER BY abs_key LIMIT 5");
        assertTrue(result.getRowCount() > 0);
        log.info("Subquery with remote functions succeeded: %d rows", result.getRowCount());
    }
}
