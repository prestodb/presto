package com.facebook.presto.hive;

import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.testing.Assertions;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class TestJsonHiveHandles
{
    private static final Map<String, Object> TABLE_HANDLE_AS_MAP = ImmutableMap.<String, Object>of(
            "clientId", "hive",
            "tableName", "schema.table");

    private static final Map<String, Object> COLUMN_HANDLE_AS_MAP = ImmutableMap.<String, Object>builder()
            .put("clientId", "hive")
            .put("name", "column")
            .put("ordinalPosition", 42)
            .put("hiveType", "FLOAT")
            .put("hiveColumnIndex", -1)
            .put("partitionKey", true)
            .build();

    private ObjectMapper objectMapper = new ObjectMapperProvider().get();

    @Test
    public void testTableHandleSerialize()
            throws Exception
    {
        HiveTableHandle tableHandle = new HiveTableHandle("hive", new SchemaTableName("schema", "table"));

        assertTrue(objectMapper.canSerialize(HiveTableHandle.class));
        String json = objectMapper.writeValueAsString(tableHandle);
        testJsonEquals(json, TABLE_HANDLE_AS_MAP);
    }

    @Test
    public void testTableHandleDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(TABLE_HANDLE_AS_MAP);

        HiveTableHandle tableHandle = objectMapper.readValue(json, HiveTableHandle.class);

        assertEquals(tableHandle.getTableName(), new SchemaTableName("schema", "table"));
    }

    @Test
    public void testColumnHandleSerialize()
            throws Exception
    {
        HiveColumnHandle columnHandle = new HiveColumnHandle("hive", "column", 42, HiveType.FLOAT, -1, true);

        assertTrue(objectMapper.canSerialize(HiveColumnHandle.class));
        String json = objectMapper.writeValueAsString(columnHandle);
        testJsonEquals(json, COLUMN_HANDLE_AS_MAP);
    }

    @Test
    public void testColumnHandleDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(COLUMN_HANDLE_AS_MAP);

        HiveColumnHandle columnHandle = objectMapper.readValue(json, HiveColumnHandle.class);

        assertEquals(columnHandle.getName(), "column");
        assertEquals(columnHandle.getOrdinalPosition(), 42);
        assertEquals(columnHandle.getHiveType(), HiveType.FLOAT);
        assertEquals(columnHandle.getHiveColumnIndex(), -1);
        assertEquals(columnHandle.isPartitionKey(), true);
    }

    private void testJsonEquals(String json, Map<String, Object> expectedMap)
            throws Exception
    {
        final Map<String, Object> jsonMap = objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
        Assertions.assertEqualsIgnoreOrder(jsonMap.entrySet(), expectedMap.entrySet());
    }
}
