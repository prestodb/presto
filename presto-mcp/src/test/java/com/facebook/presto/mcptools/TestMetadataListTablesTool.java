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
package com.facebook.presto.mcptools;

import com.facebook.presto.DummyPrestoQueryClient;
import com.facebook.presto.mcp.PrestoQueryClient;
import com.facebook.presto.mcp.mcptools.MetadataListTablesTool;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
public class TestMetadataListTablesTool
{
    private MetadataListTablesTool tool;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeMethod
    public void setup()
    {
        Map<String, List<List<Object>>> data = ImmutableMap.of(
                "SHOW TABLES FROM hive.default",
                ImmutableList.of(
                        ImmutableList.of("orders"),
                        ImmutableList.of("customers")));

        PrestoQueryClient client = new DummyPrestoQueryClient(data);
        tool = new MetadataListTablesTool(client);
    }

    @Test
    public void testListTables()
    {
        ObjectNode args = mapper.createObjectNode();
        args.put("catalog", "hive");
        args.put("schema", "default");

        JsonNode result = tool.call(args, null);

        assertEquals(result.get(0).asText(), "orders");
        assertEquals(result.get(1).asText(), "customers");
    }
}
