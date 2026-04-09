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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;

/**
 * Dispatcher for MCP (Model Context Protocol) requests.
 * Handles initialization, tool listing, and tool execution requests.
 */
public class McpDispatcher
{
    private static final String MCP_PROTOCOL_VERSION = "2024-11-05";
    private static final String SERVER_NAME = "presto-mcp";
    private static final String SERVER_VERSION = "1.0.0";
    private static final String JSONRPC_VERSION = "2.0";
    private static final int ERROR_CODE_INTERNAL = -32000;

    private final ToolRegistry toolRegistry;
    private final ObjectMapper mapper = new ObjectMapper();

    @Inject
    public McpDispatcher(ToolRegistry toolRegistry)
    {
        this.toolRegistry = toolRegistry;
    }

    /**
     * Dispatches an MCP request to the appropriate handler.
     *
     * @param request the JSON-RPC request
     * @param token optional authentication token
     * @return the JSON-RPC response, or null for notifications
     */
    public JsonNode dispatch(JsonNode request, String token)
    {
        String method = request.get("method").asText();
        JsonNode params = request.has("params") ? request.get("params") : mapper.createObjectNode();
        JsonNode idNode = request.get("id");
        Integer id = (idNode != null && !idNode.isNull()) ? idNode.asInt() : null;

        // Notifications don't need a response
        if (method.startsWith("notifications/")) {
            return null;
        }

        ObjectNode response = mapper.createObjectNode();
        response.put("jsonrpc", JSONRPC_VERSION);
        if (id != null) {
            response.put("id", id);
        }

        try {
            switch (method) {
                case "initialize":
                    response.set("result", createInitializeResult());
                    break;

                case "tools/list":
                    response.set("result", mapper.createObjectNode()
                            .set("tools", toolRegistry.listTools()));
                    break;

                case "tools/call":
                    String toolName = params.get("name").asText();
                    JsonNode args = params.has("arguments") ? params.get("arguments") : mapper.createObjectNode();
                    JsonNode toolResult = toolRegistry.callTool(toolName, args, token);
                    response.set("result", createToolCallResult(toolResult));
                    break;

                default:
                    throw new IllegalArgumentException("Unknown method: " + method);
            }
        }
        catch (Exception e) {
            ObjectNode err = mapper.createObjectNode();
            err.put("code", ERROR_CODE_INTERNAL);
            err.put("message", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            response.set("error", err);
        }

        return response;
    }

    private ObjectNode createInitializeResult()
    {
        ObjectNode initResult = mapper.createObjectNode();
        initResult.put("protocolVersion", MCP_PROTOCOL_VERSION);

        ObjectNode serverInfo = mapper.createObjectNode();
        serverInfo.put("name", SERVER_NAME);
        serverInfo.put("version", SERVER_VERSION);
        initResult.set("serverInfo", serverInfo);

        ObjectNode capabilities = mapper.createObjectNode();
        ObjectNode toolsCapability = mapper.createObjectNode();
        toolsCapability.put("listChanged", true);
        capabilities.set("tools", toolsCapability);
        initResult.set("capabilities", capabilities);

        return initResult;
    }

    private ObjectNode createToolCallResult(JsonNode toolResult)
            throws Exception
    {
        ObjectNode callResult = mapper.createObjectNode();
        callResult.set("content", mapper.createArrayNode()
                .add(mapper.createObjectNode()
                        .put("type", "text")
                        .put("text", mapper.writeValueAsString(toolResult))));
        return callResult;
    }
}
