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
package com.facebook.presto.verifier.prestoaction;

import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.verifier.retry.RetryConfig;
import org.testng.annotations.Test;

public class TestHttpNodeResourceClient
{
    private final TestingPrestoServer server;
    private final HttpNodeResourceClient client;

    public TestHttpNodeResourceClient()
            throws Exception
    {
        server = new TestingPrestoServer();
        client = new HttpNodeResourceClient(
                new JettyHttpClient(),
                new PrestoClusterConfig()
                        .setHost(server.getAddress().getHost())
                        .setHttpPort(server.getAddress().getPort()),
                new RetryConfig());
    }

    @Test
    public void testNodeResource()
    {
        client.getClusterSize("/v1/node");
    }
}
