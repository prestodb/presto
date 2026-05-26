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

import javax.inject.Inject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;

/**
 * Stdio-based MCP server for Claude Desktop integration.
 * Reads JSON-RPC requests from stdin and writes responses to stdout.
 * Supports lazy initialization to respond quickly to the first request.
 */
public class StdioMcpServer
{
    private static final String MCP_PROTOCOL_VERSION = "2024-11-05";
    private static final String SERVER_NAME = "presto-mcp";
    private static final String SERVER_VERSION = "1.0.0";
    private static final int ERROR_CODE_INTERNAL = -32603;

    private final McpDispatcher dispatcher;
    private final ObjectMapper mapper = new ObjectMapper();

    @Inject
    public StdioMcpServer(McpDispatcher dispatcher)
    {
        this.dispatcher = dispatcher;
    }

    public void run(java.io.InputStream in, java.io.PrintStream out)
    {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                PrintWriter writer = new PrintWriter(out, true)) {
            System.out.flush();
            System.err.println("Presto MCP Server ready - listening on stdio");
            System.err.flush();

            String line;
            while ((line = reader.readLine()) != null) {
                processRequest(line, writer);
            }

            System.err.println("Stdin closed, shutting down");
        }
        catch (Exception e) {
            System.err.println("Fatal error in stdio server: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private void processRequest(String line, PrintWriter writer)
    {
        try {
            if (line.trim().isEmpty()) {
                return;
            }

            JsonNode request = mapper.readTree(line);
            if (!request.has("method") || request.get("method") == null) {
                System.err.println("Received invalid request (no method): " + line);
                return;
            }

            String method = request.get("method").asText();
            System.err.println("Received request: " + method);
            System.err.flush();

            // Handle initialize specially - respond immediately even if not fully initialized
            if ("initialize".equals(method)) {
                handleInitializeRequest(request, writer);
                return;
            }

            // For other requests, use the dispatcher
            JsonNode response = dispatcher.dispatch(request, null);

            if (response != null) {
                sendResponse(response, writer);
                logResponseSent(request);
            }
            else {
                System.err.println("No response needed (notification)");
                System.err.flush();
            }
        }
        catch (InterruptedException e) {
            handleInitializationTimeout(line, writer, e);
        }
        catch (Exception e) {
            handleProcessingError(line, writer, e);
        }
    }

    private void handleInitializeRequest(JsonNode request, PrintWriter writer)
    {
        JsonNode idNode = request.get("id");
        Integer id = (idNode != null && !idNode.isNull()) ? idNode.asInt() : null;

        String initResponse = String.format(
                "{\"jsonrpc\":\"2.0\",\"id\":%d,\"result\":{\"protocolVersion\":\"%s\",\"serverInfo\":{\"name\":\"%s\",\"version\":\"%s\"},\"capabilities\":{\"tools\":{\"listChanged\":true}}}}",
                id, MCP_PROTOCOL_VERSION, SERVER_NAME, SERVER_VERSION);

        writer.println(initResponse);
        writer.flush();
        System.out.flush();
        System.err.println("Sent initialize response for request id: " + id);
        System.err.flush();
    }

    private void sendResponse(JsonNode response, PrintWriter writer)
            throws Exception
    {
        String responseJson = mapper.writeValueAsString(response);
        writer.println(responseJson);
        writer.flush();
        System.out.flush();
    }

    private void logResponseSent(JsonNode request)
    {
        JsonNode idNode = request.get("id");
        if (idNode != null) {
            System.err.println("Sent response for request id: " + idNode.asInt());
            System.err.flush();
        }
    }

    private void handleInitializationTimeout(String line, PrintWriter writer, InterruptedException e)
    {
        System.err.println("Initialization timeout: " + e.getMessage());
        e.printStackTrace(System.err);
        sendErrorResponse(line, writer, "Server still initializing, please retry");
    }

    private void handleProcessingError(String line, PrintWriter writer, Exception e)
    {
        System.err.println("Error processing request: " + e.getMessage());
        e.printStackTrace(System.err);
        String message = e.getMessage() != null ? e.getMessage().replace("\"", "\\\"") : "Internal error";
        sendErrorResponse(line, writer, message);
    }

    private void sendErrorResponse(String line, PrintWriter writer, String message)
    {
        try {
            JsonNode request = mapper.readTree(line);
            int id = request.has("id") ? request.get("id").asInt() : -1;
            String errorResponse = String.format(
                    "{\"jsonrpc\":\"2.0\",\"id\":%d,\"error\":{\"code\":%d,\"message\":\"%s\"}}",
                    id, ERROR_CODE_INTERNAL, message);
            writer.println(errorResponse);
            writer.flush();
        }
        catch (Exception ex) {
            System.err.println("Failed to send error response: " + ex.getMessage());
        }
    }
}
