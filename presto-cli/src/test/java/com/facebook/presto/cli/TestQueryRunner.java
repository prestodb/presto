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
import okhttp3.mockwebserver.MockResponse;
import org.testng.annotations.Test;

import static com.google.common.net.HttpHeaders.COOKIE;
import static com.google.common.net.HttpHeaders.LOCATION;
import static com.google.common.net.HttpHeaders.SET_COOKIE;

@Test(singleThreaded = true)
public class TestQueryRunner
        extends AbstractCliTest
{
    @Test
    public void testCookie()
            throws InterruptedException
    {
        server.enqueue(new MockResponse()
                .setResponseCode(307)
                .addHeader(LOCATION, server.url("/v1/statement"))
                .addHeader(SET_COOKIE, "a=apple"));
        server.enqueue(createMockResponse());
        server.enqueue(createMockResponse());

        executeQueries(ImmutableList.of(
                "First query will introduce a cookie;",
                "Second query should carry the cookie;"));

        assertHeaders(COOKIE, server.takeRequest().getHeaders(), ImmutableSet.of());
        assertHeaders(COOKIE, server.takeRequest().getHeaders(), ImmutableSet.of("a=apple"));
        assertHeaders(COOKIE, server.takeRequest().getHeaders(), ImmutableSet.of("a=apple"));
    }
}
