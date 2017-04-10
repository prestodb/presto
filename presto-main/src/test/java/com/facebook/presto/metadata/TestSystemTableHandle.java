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
package com.facebook.presto.metadata;

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.connector.system.SystemTableHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.airlift.json.JsonModule;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestSystemTableHandle
{
    private static final ConnectorId CONNECTOR_ID = new ConnectorId("system_connector_id");
    private static final Map<String, Object> SCHEMA_AS_MAP = ImmutableMap.of(
            "@type", "$system",
            "connectorId", CONNECTOR_ID.toString(),
            "schemaName", "system_schema",
            "tableName", "system_table");

    private ObjectMapper objectMapper;

    @BeforeMethod
    public void startUp()
    {
        Injector injector = Guice.createInjector(new JsonModule(), new HandleJsonModule());

        objectMapper = injector.getInstance(ObjectMapper.class);
    }

    @Test
    public void testSystemSerialize()
            throws Exception
    {
        SystemTableHandle internalHandle = new SystemTableHandle(CONNECTOR_ID, "system_schema", "system_table");

        assertTrue(objectMapper.canSerialize(SystemTableHandle.class));
        String json = objectMapper.writeValueAsString(internalHandle);
        testJsonEquals(json, SCHEMA_AS_MAP);
    }

    @Test
    public void testSystemDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(SCHEMA_AS_MAP);

        ConnectorTableHandle tableHandle = objectMapper.readValue(json, ConnectorTableHandle.class);
        assertEquals(tableHandle.getClass(), SystemTableHandle.class);
        SystemTableHandle systemHandle = (SystemTableHandle) tableHandle;

        assertEquals(systemHandle.getConnectorId(), CONNECTOR_ID);
        assertEquals(systemHandle.getSchemaTableName(), new SchemaTableName("system_schema", "system_table"));
    }

    private void testJsonEquals(String json, Map<String, Object> expectedMap)
            throws Exception
    {
        Map<String, Object> jsonMap = objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
        assertEqualsIgnoreOrder(jsonMap.entrySet(), expectedMap.entrySet());
    }
}
