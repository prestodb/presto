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
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.node.testing.TestingNodeModule;
import com.facebook.presto.mcp.McpDispatcher;
import com.facebook.presto.mcp.PrestoQueryClient;
import com.facebook.presto.mcp.ToolRegistry;
import com.facebook.presto.mcp.mcptools.McpTool;
import com.facebook.presto.mcp.mcptools.QueryRunTool;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.multibindings.Multibinder;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.inject.Scopes.SINGLETON;
import static org.testng.Assert.assertEquals;

public class TestToolRegistry
{
    @Test
    public void testToolRegistryLoadsQueryRunTool()
    {
        Injector injector = new Bootstrap(
                new JsonModule(),
                new TestingNodeModule(),
                binder -> {
                    Multibinder<McpTool> tools = Multibinder.newSetBinder(binder, McpTool.class);
                    tools.addBinding().toInstance(new QueryRunTool(new DummyQueryClient()));

                    binder.bind(ToolRegistry.class).in(SINGLETON);
                    binder.bind(McpDispatcher.class).in(SINGLETON);
                }
        ).initialize();

        ToolRegistry registry = injector.getInstance(ToolRegistry.class);

        JsonNode tools = registry.listTools();
        assertEquals(tools.get(0).get("name").asText(), "query_run");
    }

    // Dummy implementation for non-Presto test
    private static class DummyQueryClient
            extends PrestoQueryClient
    {
        public DummyQueryClient()
        {
            super(null);
        }

        @Override
        public List<List<Object>> runQuery(String sql, String token)
        {
            return ImmutableList.of(ImmutableList.of(1));
        }

        @Override
        public String applyLimit(String sql)
        {
            return sql;
        }
    }
}
