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

import com.facebook.presto.connector.dual.DualHandleResolver;
import com.facebook.presto.connector.dual.DualTableHandle;
import com.facebook.presto.connector.informationSchema.InformationSchemaHandleResolver;
import com.facebook.presto.connector.informationSchema.InformationSchemaTableHandle;
import com.facebook.presto.connector.system.SystemHandleResolver;
import com.facebook.presto.connector.system.SystemTableHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.tpch.TpchHandleResolver;
import com.facebook.presto.tpch.TpchTableHandle;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.Stage;
import com.google.inject.multibindings.MapBinder;
import io.airlift.json.JsonModule;
import io.airlift.testing.Assertions;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class TestJsonTableHandle
{
    private static final Map<String, Object> NATIVE_AS_MAP = ImmutableMap.<String, Object>of("type", "native",
            "schemaName", "native_schema",
            "tableName", "native_table",
            "tableId", 1);

    private static final Map<String, Object> TPCH_AS_MAP = ImmutableMap.<String, Object>of("type", "tpch",
            "tableName", "tpch_table");

    private static final Map<String, Object> SYSTEM_AS_MAP = ImmutableMap.<String, Object>of("type", "system",
            "schemaName", "system_schema",
            "tableName", "system_table");

    private static final Map<String, Object> DUAL_AS_MAP = ImmutableMap.<String, Object>of("type", "dual",
            "schemaName", "dual_schema");

    private static final Map<String, Object> INFORMATION_SCHEMA_AS_MAP = ImmutableMap.<String, Object>of("type", "information_schema",
            "catalogName", "information_schema_catalog",
            "schemaName", "information_schema_schema",
            "tableName", "information_schema_table");

    private ObjectMapper objectMapper;

    @BeforeMethod
    public void startUp()
    {
        Injector injector = Guice.createInjector(Stage.PRODUCTION,
                new JsonModule(),
                new HandleJsonModule(),
                new Module()
                {
                    @Override
                    public void configure(Binder binder)
                    {
                        MapBinder<String, ConnectorHandleResolver> connectorHandleResolverBinder = MapBinder.newMapBinder(binder, String.class, ConnectorHandleResolver.class);
                        connectorHandleResolverBinder.addBinding("native").to(NativeHandleResolver.class).in(Scopes.SINGLETON);
                        connectorHandleResolverBinder.addBinding("tpch").to(TpchHandleResolver.class).in(Scopes.SINGLETON);
                        connectorHandleResolverBinder.addBinding("system").to(SystemHandleResolver.class).in(Scopes.SINGLETON);
                        connectorHandleResolverBinder.addBinding("dual").to(DualHandleResolver.class).in(Scopes.SINGLETON);
                        connectorHandleResolverBinder.addBinding("information_schema").to(InformationSchemaHandleResolver.class).in(Scopes.SINGLETON);
                    }
                });

        objectMapper = injector.getInstance(ObjectMapper.class);
    }

    @Test
    public void testNativeSerialize()
            throws Exception
    {
        NativeTableHandle nativeHandle = new NativeTableHandle("native_schema", "native_table", 1);

        assertTrue(objectMapper.canSerialize(NativeTableHandle.class));
        String json = objectMapper.writeValueAsString(nativeHandle);
        testJsonEquals(json, NATIVE_AS_MAP);
    }

    @Test
    public void testTpchSerialize()
            throws Exception
    {
        TpchTableHandle tpchHandle = new TpchTableHandle("tpch_table");

        assertTrue(objectMapper.canSerialize(TpchTableHandle.class));
        String json = objectMapper.writeValueAsString(tpchHandle);
        testJsonEquals(json, TPCH_AS_MAP);
    }

    @Test
    public void testSystemSerialize()
            throws Exception
    {
        SystemTableHandle internalHandle = new SystemTableHandle("system_schema", "system_table");

        assertTrue(objectMapper.canSerialize(SystemTableHandle.class));
        String json = objectMapper.writeValueAsString(internalHandle);
        testJsonEquals(json, SYSTEM_AS_MAP);
    }

    @Test
    public void testDualSerialize()
            throws Exception
    {
        DualTableHandle internalHandle = new DualTableHandle("dual_schema");

        assertTrue(objectMapper.canSerialize(DualTableHandle.class));
        String json = objectMapper.writeValueAsString(internalHandle);
        testJsonEquals(json, DUAL_AS_MAP);
    }

    @Test
    public void testInformationSchemaSerialize()
            throws Exception
    {
        InformationSchemaTableHandle informationSchemaTableHandle = new InformationSchemaTableHandle(
                "information_schema_catalog",
                "information_schema_schema",
                "information_schema_table");

        assertTrue(objectMapper.canSerialize(NativeTableHandle.class));
        String json = objectMapper.writeValueAsString(informationSchemaTableHandle);
        testJsonEquals(json, INFORMATION_SCHEMA_AS_MAP);
    }

    @Test
    public void testNativeDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(NATIVE_AS_MAP);

        TableHandle tableHandle = objectMapper.readValue(json, TableHandle.class);
        assertEquals(tableHandle.getClass(), NativeTableHandle.class);
        NativeTableHandle nativeHandle = (NativeTableHandle) tableHandle;

        assertEquals(nativeHandle.getTableId(), 1);
        assertEquals(nativeHandle.getSchemaName(), "native_schema");
        assertEquals(nativeHandle.getTableName(), "native_table");
    }

    @Test
    public void testTpchDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(TPCH_AS_MAP);

        TableHandle tableHandle = objectMapper.readValue(json, TableHandle.class);
        assertEquals(tableHandle.getClass(), TpchTableHandle.class);
        TpchTableHandle tpchTableHandle = (TpchTableHandle) tableHandle;

        assertEquals(tpchTableHandle.getTableName(), "tpch_table");
    }

    @Test
    public void testSystemDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(SYSTEM_AS_MAP);

        TableHandle tableHandle = objectMapper.readValue(json, TableHandle.class);
        assertEquals(tableHandle.getClass(), SystemTableHandle.class);
        SystemTableHandle systemHandle = (SystemTableHandle) tableHandle;

        assertEquals(systemHandle.getSchemaTableName(), new SchemaTableName("system_schema", "system_table"));
    }

    @Test
    public void testDualDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(DUAL_AS_MAP);

        TableHandle tableHandle = objectMapper.readValue(json, TableHandle.class);
        assertEquals(tableHandle.getClass(), DualTableHandle.class);
        DualTableHandle dualHandle = (DualTableHandle) tableHandle;

        assertEquals(dualHandle.getSchemaName(), "dual_schema");
    }

    @Test
    public void testInformationSchemaDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(INFORMATION_SCHEMA_AS_MAP);

        TableHandle tableHandle = objectMapper.readValue(json, TableHandle.class);
        assertEquals(tableHandle.getClass(), InformationSchemaTableHandle.class);
        InformationSchemaTableHandle informationSchemaHandle = (InformationSchemaTableHandle) tableHandle;

        assertEquals(informationSchemaHandle.getCatalogName(), "information_schema_catalog");
        assertEquals(informationSchemaHandle.getSchemaName(), "information_schema_schema");
        assertEquals(informationSchemaHandle.getTableName(), "information_schema_table");
    }

    private void testJsonEquals(String json, Map<String, Object> expectedMap)
            throws Exception
    {
        final Map<String, Object> jsonMap = objectMapper.readValue(json, new TypeReference<Map<String, Object>>()
        {
        });
        Assertions.assertEqualsIgnoreOrder(jsonMap.entrySet(), expectedMap.entrySet());
    }
}
