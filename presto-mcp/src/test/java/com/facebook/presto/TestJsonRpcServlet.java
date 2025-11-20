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
import com.facebook.presto.mcp.PrestoQueryClient;
import com.facebook.presto.mcp.ToolRegistry;
import com.facebook.presto.mcp.mcptools.McpTool;
import com.facebook.presto.mcp.mcptools.QueryRunTool;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.multibindings.Multibinder;
import jakarta.servlet.Servlet;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestJsonRpcServlet
{
    private TestingHttpServer server;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeMethod
    public void setup()
    {
        Injector injector = new Bootstrap(
                new JsonModule(),
                new TestingNodeModule(),
                new TestingHttpServerModule(),
                binder -> {
                    // Dummy Presto client
                    binder.bind(PrestoQueryClient.class).toInstance(new DummyQueryClient());

                    // Register tools
                    Multibinder<McpTool> tools = newSetBinder(binder, McpTool.class);
                    tools.addBinding().to(QueryRunTool.class);

                    // Core components
                    binder.bind(ToolRegistry.class).in(SINGLETON);
                    binder.bind(McpDispatcher.class).in(SINGLETON);
                    binder.bind(JsonRpcServlet.class).in(SINGLETON);

                    // Default servlet required by TestingHttpServer
                    binder.bind(Servlet.class)
                            .annotatedWith(TheServlet.class)
                            .to(JsonRpcServlet.class)
                            .in(SINGLETON);

                    // Init params binder
                    newMapBinder(binder, String.class, String.class, TheServlet.class);

                    // Map /mcp to servlet
                    newMapBinder(binder, String.class, Servlet.class, TheServlet.class)
                            .addBinding("/mcp")
                            .to(JsonRpcServlet.class)
                            .in(SINGLETON);
                }
        ).initialize();

        server = injector.getInstance(TestingHttpServer.class);
    }

    @Test
    public void testToolsListRequest()
            throws Exception
    {
        OkHttpClient client = new OkHttpClient();
        String payload =
                "{" + "\"jsonrpc\":\"2.0\","
                        + "\"id\":1,"
                        + "\"method\":\"tools/list\","
                        + "\"params\":{}"
                        + "}";

        Request request = new Request.Builder()
                .url(server.getBaseUrl().resolve("/mcp").toURL())
                .post(RequestBody.create(payload, MediaType.get("application/json")))
                .build();

        Response response = client.newCall(request).execute();
        String body = response.body().string();

        JsonNode json = mapper.readTree(body);

        assertEquals(json.get("id").asInt(), 1);
        assertTrue(json.has("result"));
    }

    private static class DummyQueryClient
            extends PrestoQueryClient
    {
        public DummyQueryClient()
        {
            super(null);  // no Presto URI for test
        }

        @Override
        public List<List<Object>> runQuery(String sql, String token)
        {
            // deterministic response without hitting Presto
            return ImmutableList.of(ImmutableList.of("ok"));
        }

        @Override
        public String applyLimit(String sql)
        {
            return sql;
        }
    }
}
