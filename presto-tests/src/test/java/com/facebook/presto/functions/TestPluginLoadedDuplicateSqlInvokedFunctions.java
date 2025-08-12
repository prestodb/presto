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
package com.facebook.presto.functions;

import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.tests.TestingPrestoClient;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.regex.Pattern;

import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.fail;

public class TestPluginLoadedDuplicateSqlInvokedFunctions
{
    protected TestingPrestoServer server;
    protected TestingPrestoClient client;

    @BeforeClass
    public void setup()
            throws Exception
    {
        server = new TestingPrestoServer();
        server.installPlugin(new TestDuplicateFunctionsPlugin());
        client = new TestingPrestoClient(server, testSessionBuilder()
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("America/Bahia_Banderas"))
                .build());
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

    private static class TestDuplicateFunctionsPlugin
            implements Plugin
    {
        @Override
        public Set<Class<?>> getSqlInvokedFunctions()
        {
            return ImmutableSet.<Class<?>>builder()
                    .add(TestDuplicateSqlInvokedFunctions.class)
                    .build();
        }
    }

    @Test
    public void testDuplicateFunctionsLoaded()
    {
        assertInvalidFunction(JAVA_BUILTIN_NAMESPACE + ".modulo(10,3)",
                Pattern.quote(format("java.lang.IllegalArgumentException: Function already registered: %s.array_intersect<T>(array(array(T))):array(T)", JAVA_BUILTIN_NAMESPACE)));
    }
}
