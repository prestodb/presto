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
package com.facebook.presto;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.http.server.TheServlet;
import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.airlift.http.server.testing.TestingHttpServerModule;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.node.testing.TestingNodeModule;
import com.facebook.presto.mcp.JsonRpcServlet;
import com.facebook.presto.mcp.McpDispatcher;
import com.facebook.presto.mcp.McpServerConfig;
import com.facebook.presto.mcp.PrestoQueryClient;
import com.facebook.presto.mcp.ToolRegistry;
import com.facebook.presto.mcp.mcptools.McpTool;
import com.facebook.presto.mcp.mcptools.QueryRunTool;
import com.facebook.presto.plugin.memory.MemoryPlugin;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.google.inject.multibindings.Multibinder;
import jakarta.servlet.Servlet;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestMcpEndToEnd
{
    private DistributedQueryRunner presto;
    private TestingHttpServer mcpServer;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeClass
    public void setup()
            throws Exception
    {
        //
        // 1. Start Presto (embedded)
        //
        Session session = testSessionBuilder()
                .setCatalog("memory")
                .setSchema("default")
                .build();

        presto = DistributedQueryRunner.builder(session)
                .setNodeCount(1)
                .build();

        presto.installPlugin(new MemoryPlugin());
        presto.createCatalog("memory", "memory");

        presto.execute(session, "CREATE SCHEMA IF NOT EXISTS memory.default");
        presto.execute(session, "CREATE TABLE memory.default.t AS SELECT 123 x");

        URI coordinatorUri = presto.getCoordinator().getBaseUrl();

        //
        // 2. Start MCP Server (using real Airlift Bootstrap)
        //
        Injector injector = new Bootstrap(
                new JsonModule(),
                new TestingNodeModule(),
                new TestingHttpServerModule(),
                binder -> {
                    // MCP configuration
                    McpServerConfig config = new McpServerConfig()
                            .setPrestoUri(coordinatorUri)
                            .setDefaultLimit(1000);
                    binder.bind(McpServerConfig.class).toInstance(config);

                    // PrestoQueryClient → connects to real Presto
                    binder.bind(PrestoQueryClient.class).in(SINGLETON);

                    // MCP ToolRegistry + Dispatcher
                    Multibinder<McpTool> tools = newSetBinder(binder, McpTool.class);
                    tools.addBinding().to(QueryRunTool.class);

                    binder.bind(ToolRegistry.class).in(SINGLETON);
                    binder.bind(McpDispatcher.class).in(SINGLETON);

                    // Servlet binding
                    binder.bind(JsonRpcServlet.class).in(SINGLETON);
                    binder.bind(Servlet.class)
                            .annotatedWith(TheServlet.class)
                            .to(JsonRpcServlet.class)
                            .in(SINGLETON);
                    newMapBinder(binder, String.class, Servlet.class, TheServlet.class)
                            .addBinding("/mcp")
                            .to(JsonRpcServlet.class)
                            .in(SINGLETON);
                    newMapBinder(binder, String.class, String.class, TheServlet.class);
                }
        ).initialize();

        mcpServer = injector.getInstance(TestingHttpServer.class);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (mcpServer != null) {
            mcpServer.stop();
        }
        if (presto != null) {
            presto.close();
        }
    }

    @Test
    public void testQueryRunEndToEnd()
            throws Exception
    {
        OkHttpClient client = new OkHttpClient();
        //
        // Build JSON-RPC query_run payload
        //
        String payload =
                "{" + "\"jsonrpc\":\"2.0\","
                        + "\"id\":11,"
                        + "\"method\":\"tools/call\","
                        + "\"params\":{"
                        + "\"name\":\"query_run\","
                        + "\"arguments\":{"
                        + "\"sql\":\"SELECT * FROM memory.default.t\""
                        + "}"
                        + "}"
                        + "}";

        Request request = new Request.Builder()
                .url(mcpServer.getBaseUrl().resolve("/mcp").toURL())
                .post(RequestBody.create(payload, MediaType.get("application/json")))
                .build();

        Response response = client.newCall(request).execute();
        String body = response.body().string();

        JsonNode json = mapper.readTree(body);

        assertTrue(json.has("result"));
        JsonNode contentArray = json.get("result").get("content");
        String textVal = contentArray.get(0).get("text").asText();
        JsonNode parsedText = mapper.readTree(textVal);
        assertEquals(parsedText.get(0).get(0).asInt(), 123);
    }
}
