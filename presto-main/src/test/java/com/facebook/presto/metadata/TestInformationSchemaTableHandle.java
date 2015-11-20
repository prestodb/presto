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

import com.facebook.presto.connector.informationSchema.InformationSchemaHandleResolver;
import com.facebook.presto.connector.informationSchema.InformationSchemaTableHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.google.inject.multibindings.MapBinder;
import io.airlift.json.JsonModule;
import io.airlift.testing.Assertions;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestInformationSchemaTableHandle
{
    private static final String CONNECTOR_ID = "information_connector_id";
    private static final Map<String, Object> SCHEMA_AS_MAP = ImmutableMap.<String, Object>of(
            "@type", "information_schema",
            "connectorId", CONNECTOR_ID,
            "catalogName", "information_schema_catalog",
            "schemaName", "information_schema_schema",
            "tableName", "information_schema_table"
    );

    private ObjectMapper objectMapper;

    @BeforeMethod
    public void startUp()
    {
        Injector injector = Guice.createInjector(Stage.PRODUCTION,
                new JsonModule(),
                new HandleJsonModule(),
                binder -> {
                    MapBinder<String, ConnectorHandleResolver> connectorHandleResolverBinder = MapBinder.newMapBinder(binder, String.class, ConnectorHandleResolver.class);
                    connectorHandleResolverBinder.addBinding("information_schema").toInstance(new InformationSchemaHandleResolver(CONNECTOR_ID));
                });

        objectMapper = injector.getInstance(ObjectMapper.class);
    }

    @Test
    public void testInformationSchemaSerialize()
            throws Exception
    {
        InformationSchemaTableHandle informationSchemaTableHandle = new InformationSchemaTableHandle(
                CONNECTOR_ID,
                "information_schema_catalog",
                "information_schema_schema",
                "information_schema_table");

        assertTrue(objectMapper.canSerialize(InformationSchemaTableHandle.class));
        String json = objectMapper.writeValueAsString(informationSchemaTableHandle);
        testJsonEquals(json, SCHEMA_AS_MAP);
    }

    @Test
    public void testInformationSchemaDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(SCHEMA_AS_MAP);

        ConnectorTableHandle tableHandle = objectMapper.readValue(json, ConnectorTableHandle.class);
        assertEquals(tableHandle.getClass(), InformationSchemaTableHandle.class);
        InformationSchemaTableHandle informationSchemaHandle = (InformationSchemaTableHandle) tableHandle;

        assertEquals(informationSchemaHandle.getConnectorId(), CONNECTOR_ID);
        assertEquals(informationSchemaHandle.getCatalogName(), "information_schema_catalog");
        assertEquals(informationSchemaHandle.getSchemaName(), "information_schema_schema");
        assertEquals(informationSchemaHandle.getTableName(), "information_schema_table");
    }

    private void testJsonEquals(String json, Map<String, Object> expectedMap)
            throws Exception
    {
        Map<String, Object> jsonMap = objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
        Assertions.assertEqualsIgnoreOrder(jsonMap.entrySet(), expectedMap.entrySet());
    }
}
