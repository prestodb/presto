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

import com.facebook.presto.mcp.mcptools.McpTool;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Registry for managing MCP tools.
 * Provides tool discovery and execution capabilities.
 */
public class ToolRegistry
{
    private final Map<String, McpTool> tools = new HashMap<>();
    private final ObjectMapper mapper = new ObjectMapper();

    @Inject
    public ToolRegistry(Set<McpTool> toolSet)
    {
        for (McpTool tool : toolSet) {
            tools.put(tool.getName(), tool);
        }
    }

    /**
     * Lists all registered tools with their metadata.
     *
     * @return JSON array of tool definitions
     */
    public JsonNode listTools()
    {
        ArrayNode arrayNode = mapper.createArrayNode();
        for (McpTool tool : tools.values()) {
            ObjectNode node = mapper.createObjectNode();
            node.put("name", tool.getName());
            node.put("description", tool.getDescription());
            node.set("inputSchema", tool.getInputSchema());
            arrayNode.add(node);
        }
        return arrayNode;
    }

    /**
     * Executes a tool by name with the provided arguments.
     *
     * @param name the tool name
     * @param args the tool arguments as JSON
     * @param token optional authentication token
     * @return the tool execution result as JSON
     * @throws Exception if the tool is not found or execution fails
     */
    public JsonNode callTool(String name, JsonNode args, String token)
            throws Exception
    {
        McpTool tool = tools.get(name);
        if (tool == null) {
            throw new IllegalArgumentException("Tool not found: " + name);
        }
        return tool.call(args, token);
    }
}
