package com.facebook.presto.metadata;

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
            "schemaName", "schema",
            "tableName", "table",
            "tableId", 1);

    private static final Map<String, Object> TPCH_AS_MAP = ImmutableMap.<String, Object>of("type", "tpch",
            "tableName", "tpchtable");

    private static final Map<String, Object> INTERNAL_AS_MAP = ImmutableMap.<String, Object>of("type", "internal",
            "tableName", "thecatalog.theschema.thetable");

    private static final Map<String, Object> IMPORT_AS_MAP = ImmutableMap.<String, Object>of("type", "import",
            "clientId", "import",
            "tableName", "thesource.thedatabase.thetable",
            "tableHandle", TPCH_AS_MAP);

    private ObjectMapper objectMapper;

    @BeforeMethod
    public void startUp()
    {
        Injector injector = Guice.createInjector(Stage.PRODUCTION,
                new JsonModule(),
                new HandleJsonModule(),
                new Module() {
                    @Override
                    public void configure(Binder binder)
                    {
                        binder.bind(HandleResolver.class).in(Scopes.SINGLETON);
                    }
                });

        objectMapper = injector.getInstance(ObjectMapper.class);

        HandleResolver handleResolver = injector.getInstance(HandleResolver.class);
        handleResolver.addHandleResolver("native", new NativeHandleResolver());
        handleResolver.addHandleResolver("tpch", new TpchHandleResolver());
        handleResolver.addHandleResolver("internal", new InternalHandleResolver());
        handleResolver.addHandleResolver("import", new ImportHandleResolver());
    }

    @Test
    public void testNativeSerialize()
            throws Exception
    {
        NativeTableHandle nativeHandle = new NativeTableHandle("schema", "table", 1);

        assertTrue(objectMapper.canSerialize(NativeTableHandle.class));
        String json = objectMapper.writeValueAsString(nativeHandle);
        testJsonEquals(json, NATIVE_AS_MAP);
    }

    @Test
    public void testInternalSerialize()
            throws Exception
    {
        InternalTableHandle internalHandle = new InternalTableHandle(new QualifiedTableName("thecatalog", "theschema", "thetable"));

        assertTrue(objectMapper.canSerialize(InternalTableHandle.class));
        String json = objectMapper.writeValueAsString(internalHandle);
        testJsonEquals(json, INTERNAL_AS_MAP);
    }

    @Test
    public void testImportSerialize()
            throws Exception
    {
        ImportTableHandle importHandle = new ImportTableHandle("import", new QualifiedTableName("thesource", "thedatabase", "thetable"), new TpchTableHandle("tpchtable"));

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
    }

    @Test
    public void testInternalDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(INTERNAL_AS_MAP);

        TableHandle tableHandle = objectMapper.readValue(json, TableHandle.class);
        assertEquals(tableHandle.getClass(), InternalTableHandle.class);
        InternalTableHandle internalHandle = (InternalTableHandle) tableHandle;

        assertEquals(internalHandle.getTableName(), new QualifiedTableName("thecatalog", "theschema", "thetable"));
    }

    @Test
    public void testImportDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(IMPORT_AS_MAP);

        TableHandle tableHandle = objectMapper.readValue(json, TableHandle.class);
        assertEquals(tableHandle.getClass(), ImportTableHandle.class);
        ImportTableHandle importHandle = (ImportTableHandle) tableHandle;

        assertEquals(importHandle.getTableName(), new QualifiedTableName("thesource", "thedatabase", "thetable"));
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
