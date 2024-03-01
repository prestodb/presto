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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.presto.server.testing.TestingPrestoServer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.failureDetector.HeartbeatFailureDetector.Stats;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestNodeResource
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

    @AfterMethod
    public void teardown()
    {
        closeQuietly(server);
        closeQuietly(client);
    }

    @Test
    public void testGetAllNodes()
    {
        List<Stats> nodes = client.execute(
                prepareGet().setUri(server.resolve("/v1/node")).build(),
                createJsonResponseHandler(listJsonCodec(Stats.class)));

        // we only have one node and the list never contains the current node
        assertTrue(nodes.isEmpty());
    }

    @Test
    public void testGetFailedNodes()
    {
        List<Stats> nodes = client.execute(
                prepareGet().setUri(server.resolve("/v1/node/failed")).build(),
                createJsonResponseHandler(listJsonCodec(Stats.class)));

        assertTrue(nodes.isEmpty());
    }
}
