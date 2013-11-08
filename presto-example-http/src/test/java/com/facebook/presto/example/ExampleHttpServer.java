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
package com.facebook.presto.example;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.node.NodeConfig;
import io.airlift.node.NodeInfo;

import javax.servlet.Servlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;

public class ExampleHttpServer
{
    private final TestingHttpServer httpServer;

    public ExampleHttpServer()
            throws Exception
    {
        NodeConfig nodeConfig = new NodeConfig()
                .setEnvironment("test")
                .setNodeInternalIp(getV4Localhost())
                .setNodeBindIp(getV4Localhost());
        NodeInfo nodeInfo = new NodeInfo(nodeConfig);

        HttpServerConfig httpServerConfig = new HttpServerConfig();
        Servlet httpServlet = new HttpServlet()
        {
            @Override
            protected void doGet(HttpServletRequest request, HttpServletResponse response)
                    throws IOException
            {
                String pathInfo = request.getPathInfo();
                URL dataUrl = Resources.getResource(TestExampleClient.class, pathInfo);
                ByteStreams.copy(Resources.newInputStreamSupplier(dataUrl), response.getOutputStream());
            }
        };

        httpServer = new TestingHttpServer(
                new HttpServerInfo(httpServerConfig, nodeInfo),
                nodeInfo,
                httpServerConfig,
                httpServlet,
                ImmutableMap.<String, String>of());
        httpServer.start();
    }

    public void stop()
            throws Exception
    {
        httpServer.stop();
    }

    public URI getBaseUrl()
    {
        return httpServer.getBaseUrl();
    }


    // todo add TestingNodeInfo to airlift
    @SuppressWarnings("ImplicitNumericConversion")
    private static InetAddress getV4Localhost()
    {
        try {
            return InetAddress.getByAddress("localhost", new byte[]{127, 0, 0, 1});
        }
        catch (UnknownHostException e) {
            throw new AssertionError("Could not create localhost address");
        }
    }
}
