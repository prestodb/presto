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
import com.facebook.presto.scalar.sql.ArrayIntersectFunction;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.tests.TestingPrestoClient;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Set;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertThrows;

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

        @Override
        public Set<Class<?>> getFunctions()
        {
            return ImmutableSet.<Class<?>>builder()
                    .add(TestFunctions.class)
                    // Adding a SQL Invoked function in the built-in functions to mimic a conflict.
                    .add(ArrayIntersectFunction.class)
                    .build();
        }
    }

    // As soon as we trigger the conflict check with the built-in functions, an error will be thrown if duplicate signatures are found.
    // In `PrestoServer.java` this conflict check is triggered implicitly.
    @Test
    public void testDuplicateFunctionsLoaded()
    {
        assertThrows(IllegalArgumentException.class, () -> server.triggerConflictCheckWithBuiltInFunctions());
    }
}
