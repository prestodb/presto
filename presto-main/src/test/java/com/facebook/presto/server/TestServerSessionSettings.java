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
package com.facebook.presto.server;

import com.facebook.presto.client.PrestoHeaders;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.google.common.base.Charsets;
import com.google.common.net.HttpHeaders;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.testing.Closeables;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StringResponseHandler.StringResponse;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestServerSessionSettings
{
    private TestingPrestoServer server;
    private HttpClient client;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        server = new TestingPrestoServer();
        client = new JettyHttpClient();
    }

    @SuppressWarnings("deprecation")
    @AfterMethod
    public void teardown()
    {
        Closeables.closeQuietly(server);
        Closeables.closeQuietly(client);
    }

    @Test
    public void testSettingSesionsWithoutSessionId()
            throws Exception
    {
        StringResponse response = executeQuery("SET var = 3", null);
        assertEquals(response.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR.code());
    }

    @Test
    public void testSettingSesionsWithSessionId()
            throws Exception
    {
        // Set variable
        String expected = "{\"columns\":[" +
        "{\"name\":\"result\",\"type\":\"String\"}]," +
        "\"data\":[[true]]}\n";

        StringResponse response = executeQuery("SET var = 3", "test");
        assertEquals(response.getStatusCode(), HttpStatus.OK.code());
        assertEquals(response.getHeader(HttpHeaders.CONTENT_TYPE), "application/json");
        assertEquals(response.getBody(), expected);

        // Check session settings
        expected = "{\"columns\":[" +
        "{\"name\":\"result\",\"type\":\"String\"}]," +
        "\"data\":[[\"{var=3}\"]]}\n";

        response = executeQuery("SET", "test");
        assertEquals(response.getStatusCode(), HttpStatus.OK.code());
        assertEquals(response.getHeader(HttpHeaders.CONTENT_TYPE), "application/json");
        assertEquals(response.getBody(), expected);

    }

    private StringResponse executeQuery(String query, String sessionId)
    {
        Request.Builder builder = preparePost()
                .setUri(server.resolve("/v1/execute"))
                .setHeader(PrestoHeaders.PRESTO_USER, "test")
                .setHeader(PrestoHeaders.PRESTO_CATALOG, "catalog")
                .setHeader(PrestoHeaders.PRESTO_SCHEMA, "schema")
                .setHeader(PrestoHeaders.PRESTO_TIME_ZONE, "UTC")
                .setBodyGenerator(createStaticBodyGenerator(query, Charsets.UTF_8));

        if (sessionId != null) {
            builder.setHeader(PrestoHeaders.PRESTO_SESSIONID, sessionId);
        }

        Request request = builder.build();
        return client.execute(request, createStringResponseHandler());
    }
}
