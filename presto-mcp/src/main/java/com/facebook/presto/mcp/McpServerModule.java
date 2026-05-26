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

import com.facebook.airlift.http.server.TheServlet;
import com.facebook.presto.mcp.mcptools.McpTool;
import com.facebook.presto.mcp.mcptools.MetadataGetColumnsTool;
import com.facebook.presto.mcp.mcptools.MetadataListCatalogsTool;
import com.facebook.presto.mcp.mcptools.MetadataListSchemasTool;
import com.facebook.presto.mcp.mcptools.MetadataListTablesTool;
import com.facebook.presto.mcp.mcptools.QueryRunTool;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import jakarta.servlet.Servlet;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.MapBinder.newMapBinder;

/**
 * Guice module for configuring the MCP server components.
 * Registers all MCP tools and configures HTTP servlet bindings.
 */
public class McpServerModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        // Bind configuration
        configBinder(binder).bindConfig(McpServerConfig.class);

        // Bind core components
        binder.bind(PrestoQueryClient.class).in(SINGLETON);
        binder.bind(ToolRegistry.class).in(SINGLETON);
        binder.bind(McpDispatcher.class).in(SINGLETON);
        binder.bind(JsonRpcServlet.class).in(SINGLETON);
        binder.bind(StdioMcpServer.class).in(SINGLETON);

        // Register MCP tools
        Multibinder<McpTool> toolBinder = Multibinder.newSetBinder(binder, McpTool.class);
        toolBinder.addBinding().to(QueryRunTool.class);
        toolBinder.addBinding().to(MetadataListCatalogsTool.class);
        toolBinder.addBinding().to(MetadataListSchemasTool.class);
        toolBinder.addBinding().to(MetadataListTablesTool.class);
        toolBinder.addBinding().to(MetadataGetColumnsTool.class);

        // Configure HTTP servlet mappings
        // Bind as default servlet (required by HttpServerModule)
        binder.bind(Servlet.class).annotatedWith(TheServlet.class).to(JsonRpcServlet.class).in(SINGLETON);

        // Also bind to specific path
        newMapBinder(binder, String.class, Servlet.class, TheServlet.class)
                .addBinding("/mcp")
                .to(JsonRpcServlet.class);
    }
}
