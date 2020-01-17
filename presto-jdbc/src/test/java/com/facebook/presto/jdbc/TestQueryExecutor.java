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
package com.facebook.presto.jdbc;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.client.ServerInfo;
import io.airlift.units.Duration;
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.client.NodeVersion.UNKNOWN;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestQueryExecutor
{
    private static final JsonCodec<ServerInfo> SERVER_INFO_CODEC = jsonCodec(ServerInfo.class);

    private MockWebServer server;

    @BeforeMethod
    public void setup()
            throws IOException
    {
        server = new MockWebServer();
        server.start();
    }

    @AfterMethod
    public void teardown()
            throws IOException
    {
        server.close();
    }

    @Test
    public void testGetServerInfo()
            throws Exception
    {
        ServerInfo expected = new ServerInfo(UNKNOWN, "test", true, false, Optional.of(Duration.valueOf("2m")));

        server.enqueue(new MockResponse()
                .addHeader(CONTENT_TYPE, "application/json")
                .setBody(SERVER_INFO_CODEC.toJson(expected)));

        QueryExecutor executor = new QueryExecutor(new OkHttpClient());

        ServerInfo actual = executor.getServerInfo(server.url("/v1/info").uri());
        assertEquals(actual.getEnvironment(), "test");
        assertEquals(actual.getUptime(), Optional.of(Duration.valueOf("2m")));

        assertEquals(server.getRequestCount(), 1);
        assertEquals(server.takeRequest().getPath(), "/v1/info");
    }
}
