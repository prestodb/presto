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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HttpHeaders;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.testing.Closeables;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StringResponseHandler.StringResponse;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestExecuteResource
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
    public void testExecute()
            throws Exception
    {
        String expected = "{\"columns\":[" +
                "{\"name\":\"foo\",\"type\":\"integer\",\"typeSignature\":{\"rawType\":\"integer\",\"arguments\":[],\"typeArguments\":[],\"literalArguments\":[]}}," +
                "{\"name\":\"phoo\",\"type\":\"bigint\",\"typeSignature\":{\"rawType\":\"bigint\",\"arguments\":[],\"typeArguments\":[],\"literalArguments\":[]}}," +
                "{\"name\":\"bar\",\"type\":\"varchar(3)\",\"typeSignature\":" +
                    "{\"rawType\":\"varchar\",\"arguments\":[{\"kind\":\"LONG_LITERAL\",\"value\":3}],\"typeArguments\":[],\"literalArguments\":[]}}," +
                "{\"name\":\"baz\",\"type\":\"array(bigint)\",\"typeSignature\":" +
                    "{\"rawType\":\"array\",\"arguments\":[{\"kind\":\"TYPE_SIGNATURE\",\"value\":{\"rawType\":\"bigint\",\"arguments\":[],\"typeArguments\":[],\"literalArguments\":[]}}],\"typeArguments\":[{\"rawType\":\"bigint\",\"arguments\":[],\"typeArguments\":[],\"literalArguments\":[]}],\"literalArguments\":[]}}]," +
                "\"data\":[[123, 12300000000, \"abc\",[42,44]]]}\n";
        StringResponse response = executeQuery("SELECT 123 foo, 12300000000 phoo, 'abc' bar, CAST(JSON_PARSE('[42,44]') AS ARRAY<BIGINT>) baz");
        assertEquals(response.getStatusCode(), HttpStatus.OK.code());
        assertEquals(response.getHeader(HttpHeaders.CONTENT_TYPE), "application/json");

        ObjectMapper mapper = new ObjectMapper();
        assertEquals(mapper.readTree(response.getBody()), mapper.readTree(expected));
    }

    private StringResponse executeQuery(String query)
    {
        Request request = preparePost()
                .setUri(server.resolve("/v1/execute"))
                .setHeader(PrestoHeaders.PRESTO_USER, "test")
                .setHeader(PrestoHeaders.PRESTO_CATALOG, "catalog")
                .setHeader(PrestoHeaders.PRESTO_SCHEMA, "schema")
                .setHeader(PrestoHeaders.PRESTO_TIME_ZONE, "UTC")
                .setBodyGenerator(createStaticBodyGenerator(query, UTF_8))
                .build();
        return client.execute(request, createStringResponseHandler());
    }
}
