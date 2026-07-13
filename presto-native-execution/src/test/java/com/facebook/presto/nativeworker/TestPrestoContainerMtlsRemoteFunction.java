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
package com.facebook.presto.nativeworker;

import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;

/**
 * These tests call remote functions served by the Presto Function Server
 * (implementation: {@link com.facebook.presto.server.FunctionServer}).
 */

public class TestPrestoContainerMtlsRemoteFunction
        extends AbstractTestQueryFramework
{
    @Override
    protected ContainerQueryRunner createQueryRunner()
            throws Exception
    {
        return new ContainerQueryRunner(
                ContainerQueryRunner.DEFAULT_COORDINATOR_PORT,
                ContainerQueryRunner.TPCH_CATALOG,
                ContainerQueryRunner.TINY_SCHEMA,
                ContainerQueryRunner.DEFAULT_NUMBER_OF_WORKERS,
                ContainerQueryRunner.DEFAULT_FUNCTION_SERVER_HTTPS_PORT,
                true,
                true,
                Optional.of(ContainerQueryRunner.JWT_SHARED_SECRET));
    }

    @Test
    public void testRemoteBasicTestsWithMtlsAndJwt()
    {
        assertEquals(computeActual(
                "SELECT remote.default.sqrt(n_nationkey) FROM tpch.sf1.nation LIMIT 5").getRowCount(), 5);

        assertEquals(computeActual(
                "SELECT remote.default.mod(o_orderkey, 10) FROM orders LIMIT 5").getRowCount(), 5);
    }

    @Test
    public void testRemoteFunctionResultWithMtlsAndJwt()
    {
        assertQueryWithSameQueryRunner(
                "SELECT remote.default.sqrt(n_nationkey) FROM tpch.tiny.nation LIMIT 5",
                "SELECT sqrt(n_nationkey) FROM tpch.tiny.nation LIMIT 5");
    }
}
