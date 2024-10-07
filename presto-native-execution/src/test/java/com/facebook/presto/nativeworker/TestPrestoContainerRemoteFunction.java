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

import static org.testng.Assert.assertEquals;

/**
 * These tests call remote functions served by the Presto Function Server
 * (implementation: {@link com.facebook.presto.server.FunctionServer}).
 */

public class TestPrestoContainerRemoteFunction
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
                ContainerQueryRunner.DEFAULT_FUNCTION_SERVER_PORT,
                true);
    }

    @Test
    public void testRemoteBasicTests()
    {
        assertEquals(
                computeActual("select remote.default.abs(-10)")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "10");
        assertEquals(
                computeActual("select remote.default.abs(-1230)")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "1230");
        assertEquals(
                computeActual("select remote.default.day(interval '2' day)")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "2");
        assertEquals(
                computeActual("select remote.default.length(CAST('AB' AS VARBINARY))")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "2");
        assertEquals(
                computeActual("select remote.default.floor(100000.99)")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "100000.0");
        assertEquals(
                computeActual("select remote.default.to_base32(CAST('abc' AS VARBINARY))")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "MFRGG===");
    }

    @Test
    public void testRemoteFunctionAppliedToColumn()
    {
        assertQueryWithSameQueryRunner(
                "SELECT remote.default.floor(o_totalprice) FROM tpch.sf1.orders",
                "SELECT floor(o_totalprice) FROM tpch.sf1.orders");
        assertEquals(computeActual("SELECT remote.default.floor(o_totalprice) FROM tpch.sf1.orders")
                .getMaterializedRows().size(), 1500000);
        assertEquals(computeActual("SELECT remote.default.to_base32(CAST(o_comment AS VARBINARY)) FROM tpch.sf10.orders")
                .getMaterializedRows().size(), 15000000);
        assertQueryWithSameQueryRunner(
                "SELECT remote.default.abs(l_discount) FROM tpch.sf1.lineitem",
                "SELECT abs(l_discount) FROM tpch.sf1.lineitem");
    }
}
