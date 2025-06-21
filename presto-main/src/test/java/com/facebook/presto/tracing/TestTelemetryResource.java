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
package com.facebook.presto.tracing;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.presto.common.TracingConfig;
import com.facebook.presto.server.testing.TestingPrestoServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static com.facebook.airlift.http.client.Request.Builder.preparePut;
import static com.facebook.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.server.RequestHelpers.setContentTypeHeaders;
import static org.testng.Assert.assertEquals;

public class TestTelemetryResource
{
    private HttpClient client;
    private TestingPrestoServer server;

    @BeforeClass
    public void setup()
            throws Exception
    {
        client = new JettyHttpClient();
        server = new TestingPrestoServer();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(server);
        closeQuietly(client);
        server = null;
        client = null;
    }

    @Test
    public void testUpdateTelemetryConfig()
    {
        Request.Builder requestBuilder = setContentTypeHeaders(false, preparePut());
        Request request = requestBuilder.setUri(server.getBaseUrl().resolve("/v1/telemetry/config"))
                .setBodyGenerator(jsonBodyGenerator(jsonCodec(TracingConfig.class), new TracingConfig(true)))
                .build();
        assertEquals(client.execute(request, createStringResponseHandler()).getStatusCode(), 200);
    }
}
