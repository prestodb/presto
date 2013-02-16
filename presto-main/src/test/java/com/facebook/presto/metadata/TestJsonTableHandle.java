package com.facebook.presto.metadata;

import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;

import com.facebook.presto.tpch.TpchTableHandle;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Stage;
import io.airlift.json.JsonModule;
import io.airlift.testing.Assertions;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.metadata.TableHandleJacksonModule.bindTableHandle;
import static io.airlift.json.JsonBinder.jsonBinder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class TestJsonTableHandle
{
    private static final Map<String, Object> NATIVE_AS_MAP = ImmutableMap.<String, Object>of("type", "native",
            "tableId", 1);

    private static final Map<String, Object> TPCH_AS_MAP = ImmutableMap.<String, Object>of("type", "tpch",
            "tableName", "tpchtable");

    private static final Map<String, Object> INTERNAL_AS_MAP = ImmutableMap.<String, Object>of("type", "internal",
            "catalogName", "thecatalog",
            "schemaName", "theschema",
            "tableName", "thetable");

    private static final Map<String, Object> IMPORT_AS_MAP = ImmutableMap.<String, Object>of("type", "import",
            "sourceName", "thesource",
            "databaseName", "thedatabase",
            "tableName", "thetable");

    private ObjectMapper objectMapper;

    @BeforeMethod
    public void startUp()
    {
        Injector inj = Guice.createInjector(Stage.PRODUCTION,
                new JsonModule(),
                new TableHandleModule());

        objectMapper = inj.getInstance(ObjectMapper.class);
    }

    @Test
    public void testNativeSerialize()
            throws Exception
    {
        NativeTableHandle nativeHandle = new NativeTableHandle(1);

        assertTrue(objectMapper.canSerialize(NativeTableHandle.class));
        String json = objectMapper.writeValueAsString(nativeHandle);
        testJsonEquals(json, NATIVE_AS_MAP);
    }

    @Test
    public void testInternalSerialize()
            throws Exception
    {
        InternalTableHandle internalHandle = new InternalTableHandle("thecatalog", "theschema", "thetable");

        assertTrue(objectMapper.canSerialize(InternalTableHandle.class));
        String json = objectMapper.writeValueAsString(internalHandle);
        testJsonEquals(json, INTERNAL_AS_MAP);
    }

    @Test
    public void testImportSerialize()
            throws Exception
    {
        ImportTableHandle importHandle = new ImportTableHandle("thesource", "thedatabase", "thetable");

        assertTrue(objectMapper.canSerialize(ImportTableHandle.class));
        String json = objectMapper.writeValueAsString(importHandle);
        testJsonEquals(json, IMPORT_AS_MAP);
    }

    @Test
    public void testTpchSerialize()
            throws Exception
    {
        TpchTableHandle tpchHandle = new TpchTableHandle("tpchtable");

        assertTrue(objectMapper.canSerialize(TpchTableHandle.class));
        String json = objectMapper.writeValueAsString(tpchHandle);
        testJsonEquals(json, TPCH_AS_MAP);
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
        assertEquals(nativeHandle.getDataSourceType(), DataSourceType.NATIVE);
    }

    @Test
    public void testInternalDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(INTERNAL_AS_MAP);

        TableHandle tableHandle = objectMapper.readValue(json, TableHandle.class);
        assertEquals(tableHandle.getClass(), InternalTableHandle.class);
        InternalTableHandle internalHandle = (InternalTableHandle) tableHandle;

        assertEquals(internalHandle.getDataSourceType(), DataSourceType.INTERNAL);
        assertEquals(internalHandle.getCatalogName(), "thecatalog");
        assertEquals(internalHandle.getSchemaName(), "theschema");
        assertEquals(internalHandle.getTableName(), "thetable");
    }

    @Test
    public void testImportDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(IMPORT_AS_MAP);

        TableHandle tableHandle = objectMapper.readValue(json, TableHandle.class);
        assertEquals(tableHandle.getClass(), ImportTableHandle.class);
        ImportTableHandle importHandle = (ImportTableHandle) tableHandle;

        assertEquals(importHandle.getDataSourceType(), DataSourceType.IMPORT);
        assertEquals(importHandle.getSourceName(), "thesource");
        assertEquals(importHandle.getDatabaseName(), "thedatabase");
        assertEquals(importHandle.getTableName(), "thetable");
    }

    @Test
    public void testTpchDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(TPCH_AS_MAP);

        TableHandle tableHandle = objectMapper.readValue(json, TableHandle.class);
        assertEquals(tableHandle.getClass(), TpchTableHandle.class);
        TpchTableHandle tpchTableHandle = (TpchTableHandle) tableHandle;

        assertEquals(tpchTableHandle.getTableName(), "tpchtable");
    }

    private void testJsonEquals(String json, Map<String, Object> expectedMap)
            throws Exception
    {
        final Map<String, Object> jsonMap = objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {
        });
        Assertions.assertEqualsIgnoreOrder(jsonMap.entrySet(), expectedMap.entrySet());
    }
}
