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
import com.google.inject.Inject;

import java.util.List;

import static java.util.stream.Collectors.toList;

public class MetadataListCatalogsTool
        implements McpTool
{
    private final PrestoQueryClient prestoClient;
    private final ObjectMapper mapper = new ObjectMapper();

    @Inject
    public MetadataListCatalogsTool(PrestoQueryClient prestoClient)
    {
        this.prestoClient = prestoClient;
    }

    @Override
    public String getName()
    {
        return "metadata_listCatalogs";
    }

    @Override
    public String getDescription()
    {
        return "Return the list of available catalogs in Presto.";
    }

    @Override
    public JsonNode getInputSchema()
    {
        return mapper.createObjectNode()
                .put("type", "object");
    }

    @Override
    public JsonNode call(JsonNode arguments, String token)
    {
        List<List<Object>> rows = prestoClient.runQuery("SHOW CATALOGS", token);

        List<String> catalogs = rows.stream()
                .map(r -> r.get(0).toString())
                .collect(toList());

        return mapper.valueToTree(catalogs);
    }
}
