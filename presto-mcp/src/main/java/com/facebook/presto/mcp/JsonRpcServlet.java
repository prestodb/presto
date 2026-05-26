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
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import javax.inject.Inject;

import java.io.IOException;

/**
 * HTTP servlet for handling JSON-RPC requests to the MCP server.
 * Accepts POST requests with JSON-RPC payloads and returns JSON-RPC responses.
 */
public class JsonRpcServlet
        extends HttpServlet
{
    private static final String BEARER_PREFIX = "Bearer ";
    private static final String CONTENT_TYPE_JSON = "application/json";

    private final ObjectMapper mapper = new ObjectMapper();
    private final McpDispatcher dispatcher;

    @Inject
    public JsonRpcServlet(McpDispatcher dispatcher)
    {
        this.dispatcher = dispatcher;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws IOException
    {
        JsonNode rpcRequest = mapper.readTree(req.getInputStream());
        String authHeader = req.getHeader("Authorization");

        String token = extractBearerToken(authHeader);
        JsonNode rpcResponse = dispatcher.dispatch(rpcRequest, token);

        resp.setContentType(CONTENT_TYPE_JSON);
        mapper.writeValue(resp.getOutputStream(), rpcResponse);
    }

    private String extractBearerToken(String authHeader)
    {
        if (authHeader != null && authHeader.startsWith(BEARER_PREFIX)) {
            return authHeader.substring(BEARER_PREFIX.length());
        }
        return null;
    }
}
