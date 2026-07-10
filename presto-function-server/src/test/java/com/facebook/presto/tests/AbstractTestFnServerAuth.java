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
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

/**
 * Abstract base class containing all valid Function Server authentication test cases.
 * Each subclass supplies a {@link #createQueryRunner()} wired for a specific auth
 * configuration (e.g. mTLS + JWT, mTLS only) and cluster type (Java workers or C++ workers).
 */
public abstract class AbstractTestFnServerAuth
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(AbstractTestFnServerAuth.class);

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
     * Verifies that a remote function executes successfully against table data.
     * Covers the worker → Function Server call path.
     */
    @Test
    public void testRemoteFunctionOnTableData()
    {
        log.info("TEST: Remote function execution against table data");
        MaterializedResult result = computeActual(
                "SELECT rest.default.sqrt(n_nationkey) FROM nation LIMIT 5");
        assertEquals(result.getRowCount(), 5);
        log.info("Table query succeeded: %d rows returned", result.getRowCount());
    }

    /**
     * Verifies that a simple scalar remote function executes successfully.
     */
    @Test
    public void testRemoteFunctionScalar()
    {
        log.info("TEST: Simple scalar remote function execution");
        MaterializedResult result = computeActual("SELECT rest.default.abs(-123)");
        assertEquals(result.getOnlyValue(), 123);
        log.info("Scalar function call succeeded: abs(-123) = %s", result.getOnlyValue());
    }
}
