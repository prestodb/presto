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
package com.facebook.presto.hive;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.StandardTypes;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class TestJsonHiveHandles
{
    private static final Map<String, Object> TABLE_HANDLE_AS_MAP = ImmutableMap.<String, Object>of(
            "clientId", "hive",
            "schemaName", "hive_schema",
            "tableName", "hive_table");

    private static final Map<String, Object> COLUMN_HANDLE_AS_MAP = ImmutableMap.<String, Object>builder()
            .put("clientId", "hive")
            .put("name", "column")
            .put("hiveType", "float")
            .put("typeSignature", "double")
            .put("hiveColumnIndex", -1)
            .put("columnType", PARTITION_KEY.toString())
            .build();

    private final ObjectMapper objectMapper = new ObjectMapperProvider().get();

    @Test
    public void testTableHandleSerialize()
            throws Exception
    {
        HiveTableHandle tableHandle = new HiveTableHandle("hive", "hive_schema", "hive_table");

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

        assertEquals(tableHandle.getClientId(), "hive");
        assertEquals(tableHandle.getSchemaName(), "hive_schema");
        assertEquals(tableHandle.getTableName(), "hive_table");
        assertEquals(tableHandle.getSchemaTableName(), new SchemaTableName("hive_schema", "hive_table"));
    }

    @Test
    public void testColumnHandleSerialize()
            throws Exception
    {
        HiveColumnHandle columnHandle = new HiveColumnHandle("hive", "column", HiveType.HIVE_FLOAT, parseTypeSignature(StandardTypes.DOUBLE), -1, PARTITION_KEY);

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

        assertEquals(columnHandle.getClientId(), "hive");
        assertEquals(columnHandle.getName(), "column");
        assertEquals(columnHandle.getTypeSignature(), DOUBLE.getTypeSignature());
        assertEquals(columnHandle.getHiveType(), HiveType.HIVE_FLOAT);
        assertEquals(columnHandle.getHiveColumnIndex(), -1);
        assertEquals(columnHandle.isPartitionKey(), true);
    }

    private void testJsonEquals(String json, Map<String, Object> expectedMap)
            throws Exception
    {
        Map<String, Object> jsonMap = objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
        assertEqualsIgnoreOrder(jsonMap.entrySet(), expectedMap.entrySet());
    }
}
