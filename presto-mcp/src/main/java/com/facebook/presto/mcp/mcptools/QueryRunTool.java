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
package com.facebook.presto.mcp.mcptools;

import com.facebook.presto.mcp.PrestoQueryClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;

import java.util.List;

public class QueryRunTool
        implements McpTool
{
    private final PrestoQueryClient prestoClient;
    private final ObjectMapper mapper = new ObjectMapper();

    @Inject
    public QueryRunTool(PrestoQueryClient prestoClient)
    {
        this.prestoClient = prestoClient;
    }

    @Override
    public String getName()
    {
        return "query_run";
    }

    @Override
    public String getDescription()
    {
        return "Execute a read-only SQL query (SELECT, SHOW, DESCRIBE, EXPLAIN) against Presto and return results. Write operations (INSERT, UPDATE, DELETE, CREATE, DROP, ALTER) are not allowed.";
    }

    @Override
    public JsonNode getInputSchema()
    {
        ObjectNode schema = mapper.createObjectNode();
        schema.put("type", "object");

        ObjectNode props = mapper.createObjectNode();
        props.set("sql", mapper.createObjectNode().put("type", "string"));

        schema.set("properties", props);
        schema.putArray("required").add("sql");

        return schema;
    }

    @Override
    public JsonNode call(JsonNode arguments, String token)
    {
        String sql = arguments.get("sql").asText();
        validateReadOnlyQuery(sql);
        String limitedSql = prestoClient.applyLimit(sql);

        List<List<Object>> rows = prestoClient.runQuery(limitedSql, token);
        return mapper.valueToTree(rows);
    }

    /**
     * Validates that the SQL query is read-only (SELECT, SHOW, DESCRIBE, EXPLAIN).
     * Rejects write operations (INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, TRUNCATE, GRANT, REVOKE).
     *
     * @param sql the SQL query to validate
     * @throws IllegalArgumentException if the query contains write operations
     */
    private void validateReadOnlyQuery(String sql)
    {
        String trimmedSql = sql.trim().toLowerCase();

        while (trimmedSql.startsWith("--") || trimmedSql.startsWith("/*")) {
            if (trimmedSql.startsWith("--")) {
                int newlineIndex = trimmedSql.indexOf('\n');
                if (newlineIndex == -1) {
                    trimmedSql = "";
                    break;
                }
                trimmedSql = trimmedSql.substring(newlineIndex + 1).trim();
            }
            else if (trimmedSql.startsWith("/*")) {
                int endIndex = trimmedSql.indexOf("*/");
                if (endIndex == -1) {
                    throw new IllegalArgumentException("Unclosed comment in SQL query");
                }
                trimmedSql = trimmedSql.substring(endIndex + 2).trim();
            }
        }
        // Check for write operations
        String[] writeOperations = {
            "insert", "update", "delete", "create", "drop", "alter",
            "truncate", "grant", "revoke", "merge", "call"
        };
        for (String operation : writeOperations) {
            if (trimmedSql.startsWith(operation + " ") || trimmedSql.equals(operation)) {
                throw new IllegalArgumentException(
                        String.format("Write operation '%s' is not allowed. Only read-only queries (SELECT, SHOW, DESCRIBE, EXPLAIN) are permitted.", operation.toUpperCase()));
            }
        }
        // Allow read-only operations
        String[] readOnlyOperations = {"select", "show", "describe", "explain", "with"};
        boolean isReadOnly = false;
        for (String operation : readOnlyOperations) {
            if (trimmedSql.startsWith(operation + " ") || trimmedSql.equals(operation)) {
                isReadOnly = true;
                break;
            }
        }
        if (!isReadOnly) {
            throw new IllegalArgumentException("Invalid SQL query. Only read-only queries (SELECT, SHOW, DESCRIBE, EXPLAIN) are allowed.");
        }
    }
}
