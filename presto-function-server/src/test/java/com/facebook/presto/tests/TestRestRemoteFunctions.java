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

import com.facebook.presto.Session;
import com.facebook.presto.server.FunctionServerQueryRunner;
import com.facebook.presto.server.TestingFunctionServer;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestRestRemoteFunctions
        extends AbstractTestQueryFramework
{
    private TestingFunctionServer functionServer;
    private static final Session session = testSessionBuilder()
            .setSource("test")
            .setCatalog("tpch")
            .setSchema("tiny")
            .setSystemProperty("remote_functions_enabled", "true")
            .build();
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        int functionServerPort = findRandomPort();
        functionServer = new TestingFunctionServer(functionServerPort);
        return FunctionServerQueryRunner.createQueryRunner(
                functionServerPort,
                ImmutableMap.of("list-built-in-functions-only", "false"));
    }

    @Test
    public void testShowFunction()
    {
        MaterializedResult actualResult = computeActual(session, "show functions like '%rest.%'");
        List<MaterializedRow> actualRows = actualResult.getMaterializedRows();
        assertFalse(actualRows.isEmpty(), "Expected at least one function matching 'rest.%', but found none.");
    }

    @Test
    public void testRemoteFunctions()
    {
        assertEquals(
                computeActual(session, "select rest.default.abs(-1230)")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "1230");
        assertEquals(
                computeActual(session, "select rest.default.day(interval '2' day)")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "2");
        assertEquals(
                computeActual(session, "select rest.default.length(CAST('AB' AS VARBINARY))")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "2");
        assertEquals(
                computeActual(session, "select rest.default.floor(100000.99)")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "100000.0");
    }

    @Test
    public void testFunctionPlugins()
    {
        functionServer.installPlugin(new DummyPlugin());
        MaterializedResult actualResult = computeActual(session, "show functions like '%rest.default.is_positive%'");
        List<MaterializedRow> actualRows = actualResult.getMaterializedRows();
        assertFalse(actualRows.isEmpty());

        assertEquals(
                computeActual(session, "SELECT rest.default.is_positive(1)")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "true");
        assertEquals(
                computeActual(session, "SELECT rest.default.is_positive(-1)")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "false");
    }

    private static final class DummyPlugin
            implements Plugin
    {
        @Override
        public Set<Class<?>> getFunctions()
        {
            return ImmutableSet.of(DummyFunctions.class);
        }
    }

    public static class DummyFunctions
    {
        @ScalarFunction
        @Description("FizzBuzz")
        @SqlType(BOOLEAN)
        public static boolean isPositive(@SqlType(BIGINT) long input)
        {
            return input > 0;
        }
    }

    private static int findRandomPort()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
