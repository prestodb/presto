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
package com.facebook.presto.cli;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_ADDED_SESSION_FUNCTION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_REMOVED_SESSION_FUNCTION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION_FUNCTION;

@Test
public class TestTemporaryFunctions
        extends AbstractCliTest
{
    @Test
    public void testAddAndDropTempFunctions()
            throws InterruptedException
    {
        server.enqueue(createMockResponse().addHeader(PRESTO_ADDED_SESSION_FUNCTION, "foo=foofunction"));
        server.enqueue(createMockResponse().addHeader(PRESTO_ADDED_SESSION_FUNCTION, "bar=barfunction"));
        server.enqueue(createMockResponse());
        server.enqueue(createMockResponse().addHeader(PRESTO_REMOVED_SESSION_FUNCTION, "foo"));
        server.enqueue(createMockResponse());
        server.enqueue(createMockResponse().addHeader(PRESTO_REMOVED_SESSION_FUNCTION, "bar"));
        server.enqueue(createMockResponse());

        executeQueries(ImmutableList.of(
                "CREATE TEMPORARY FUNCTION foo() RETURNS int RETURN 1;",
                "CREATE TEMPORARY FUNCTION bar() RETURNS int RETURN 2;",
                "SELECT foo();",
                "DROP TEMPORARY FUNCTION foo();",
                "SELECT bar();",
                "DROP TEMPORARY FUNCTION bar();",
                "SELECT bar();"));

        assertHeaders(PRESTO_SESSION_FUNCTION, server.takeRequest().getHeaders(), ImmutableSet.of());
        assertHeaders(PRESTO_SESSION_FUNCTION, server.takeRequest().getHeaders(), ImmutableSet.of("foo=foofunction"));
        assertHeaders(PRESTO_SESSION_FUNCTION, server.takeRequest().getHeaders(), ImmutableSet.of("foo=foofunction", "bar=barfunction"));
        assertHeaders(PRESTO_SESSION_FUNCTION, server.takeRequest().getHeaders(), ImmutableSet.of("foo=foofunction", "bar=barfunction"));
        assertHeaders(PRESTO_SESSION_FUNCTION, server.takeRequest().getHeaders(), ImmutableSet.of("bar=barfunction"));
        assertHeaders(PRESTO_SESSION_FUNCTION, server.takeRequest().getHeaders(), ImmutableSet.of("bar=barfunction"));
        assertHeaders(PRESTO_SESSION_FUNCTION, server.takeRequest().getHeaders(), ImmutableSet.of());
    }
}
