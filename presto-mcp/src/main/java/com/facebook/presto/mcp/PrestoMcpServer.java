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
package com.facebook.presto.mcp;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.event.client.EventModule;
import com.facebook.airlift.http.server.HttpServerModule;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.node.NodeModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;

import java.io.InputStream;
import java.io.PrintStream;

/**
 * Main entry point for the Presto MCP (Model Context Protocol) server.
 * Supports two modes:
 * - STDIO mode: For integration with Claude Desktop and other MCP clients
 * - HTTP mode: For direct HTTP access to the MCP API
 */
public class PrestoMcpServer
{
    private static final Logger LOG = Logger.get(PrestoMcpServer.class);

    private PrestoMcpServer() {}

    /**
     * Starts the MCP server in HTTP mode.
     * Listens on the configured HTTP port for JSON-RPC requests.
     */
    public static void startHttp()
    {
        Bootstrap app = new Bootstrap(
                ImmutableList.<Module>builder()
                    .add(new NodeModule())
                    .add(new HttpServerModule())
                    .add(new JsonModule())
                    .add(new EventModule())
                    .add(new McpServerModule())
                .build());

        try {
            app.initialize();
            LOG.info("======== Presto MCP HTTP SERVER STARTED ========");
        }
        catch (Throwable t) {
            LOG.error(t);
            System.exit(1);
        }
    }

    /**
     * Starts the MCP server in STDIO mode.
     * Reads JSON-RPC requests from stdin and writes responses to stdout.
     */
    public static void startStdio()
    {
        try {
            // Save original streams because Airlift logging initialization intercepts them
            InputStream originalIn = System.in;
            PrintStream originalOut = System.out;
            PrintStream originalErr = System.err;

            // Redirect System.out to System.err so all Airlift standard config/logging goes to stderr.
            // Claude expects logs on stderr, and STRICTLY only JSON-RPC messages on stdout.
            System.setOut(originalErr);

            originalErr.println("======== Presto MCP STDIO SERVER STARTING ========");
            originalErr.flush();

            Bootstrap app = new Bootstrap(
                    ImmutableList.<Module>builder()
                        .add(new NodeModule())
                        .add(new JsonModule())
                        .add(new McpServerModule())
                    .build());

            Injector injector = app.initialize();
            originalErr.println("======== Presto MCP STDIO SERVER INITIALIZED ========");
            originalErr.flush();

            StdioMcpServer stdioServer = injector.getInstance(StdioMcpServer.class);
            stdioServer.run(originalIn, originalOut);
        }
        catch (Throwable t) {
            System.err.println("Error starting stdio server: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(1);
        }
    }

    public static void main(String[] args)
    {
        // Determine mode: stdio (for Claude Desktop) or HTTP (for direct access)
        boolean stdioMode = System.console() == null ||
                           "true".equals(System.getProperty("mcp.stdio")) ||
                           "stdio".equals(System.getProperty("mcp.mode"));

        if (stdioMode) {
            System.err.println("Starting in STDIO mode (for Claude Desktop)");
            startStdio();
        }
        else {
            System.err.println("Starting in HTTP mode");
            startHttp();
        }
    }
}
