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

import static java.util.stream.Collectors.toList;

public class MetadataGetColumnsTool
        implements McpTool
{
    private final PrestoQueryClient prestoClient;
    private final ObjectMapper mapper = new ObjectMapper();

    @Inject
    public MetadataGetColumnsTool(PrestoQueryClient prestoClient)
    {
        this.prestoClient = prestoClient;
    }

    @Override
    public String getName()
    {
        return "metadata_getColumns";
    }

    @Override
    public String getDescription()
    {
        return "Return column metadata for a given table.";
    }

    @Override
    public JsonNode getInputSchema()
    {
        ObjectNode schema = mapper.createObjectNode();
        schema.put("type", "object");

        ObjectNode props = mapper.createObjectNode();
        props.putObject("catalog").put("type", "string");
        props.putObject("schema").put("type", "string");
        props.putObject("table").put("type", "string");

        schema.set("properties", props);
        schema.putArray("required").add("catalog").add("schema").add("table");

        return schema;
    }

    @Override
    public JsonNode call(JsonNode arguments, String token)
    {
        String catalog = arguments.get("catalog").asText();
        String schema = arguments.get("schema").asText();
        String table = arguments.get("table").asText();

        String sql = "DESCRIBE " + catalog + "." + schema + "." + table;

        List<List<Object>> rows = prestoClient.runQuery(sql, token);

        List<ObjectNode> cols = rows.stream().map(r -> {
            ObjectNode col = mapper.createObjectNode();
            col.put("name", r.get(0).toString());
            col.put("type", r.get(1).toString());
            return col;
        }).collect(toList());

        return mapper.valueToTree(cols);
    }
}
