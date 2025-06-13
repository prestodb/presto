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
package com.facebook.presto.tests.function;

import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.TestingPrestoClient;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Key;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestScalarFunctions
{
    public static final String TEST_FUNCTION_NAMESPACE = "test.namespace";

    protected TestingPrestoServer server;
    protected TestingPrestoClient client;
    protected TypeManager typeManager;

    @BeforeClass
    public void setup()
            throws Exception
    {
        server = new TestingPrestoServer();
        server.installPlugin(new TestFunctionPlugin());
        client = new TestingPrestoClient(server, testSessionBuilder()
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("America/Bahia_Banderas"))
                .build());
        typeManager = server.getInstance(Key.get(TypeManager.class));
    }

    public void assertInvalidFunction(String expr, String exceptionPattern)
    {
        try {
            client.execute("SELECT " + expr);
            fail("Function expected to fail but not");
        }
        catch (Exception e) {
            if (!(e.getMessage().matches(exceptionPattern))) {
                fail(format("Expected exception message '%s' to match '%s' but not",
                        e.getMessage(), exceptionPattern));
            }
        }
    }

    private class TestFunctionPlugin
            implements Plugin
    {
        @Override
        public Set<Class<?>> getFunctions()
        {
            return ImmutableSet.<Class<?>>builder()
                    .add(TestFunctions.class)
                    .build();
        }
    }

    public void check(@Language("SQL") String query, Type expectedType, Object expectedValue)
    {
        MaterializedResult result = client.execute(query).getResult();
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getTypes().get(0), expectedType);
        Object actual = result.getMaterializedRows().get(0).getField(0);
        assertEquals(actual, expectedValue);
    }

    @Test
    public void testNewFunctionNamespaceFunction()
    {
        check("SELECT test.namespace.modulo(10,3)", BIGINT, 1L);
        check("SELECT test.namespace.identity('test-functions')", VARCHAR, "test-functions");
    }

    @Test
    public void testInvalidFunctionAndNamespace()
    {
        assertInvalidFunction("invalid.namespace.modulo(10,3)", "line 1:8: Function invalid.namespace.modulo not registered");
        assertInvalidFunction("test.namespace.dummy(10)", "line 1:8: Function test.namespace.dummy not registered");
    }
}
