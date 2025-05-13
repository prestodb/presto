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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.type.TypeManager;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.iceberg.TestNestedFieldConverter.nestedField;
import static com.facebook.presto.iceberg.TestNestedFieldConverter.prestoIcebergNestedField;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static java.util.Collections.emptySet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestSchemaConverter
{
    @Test
    public void testToPrestoSchema()
    {
        // Create a test TypeManager
        TypeManager typeManager = createTestFunctionAndTypeManager();

        // Create a mock Schema
        Schema mockSchema = schema();

        PrestoIcebergSchema expectedPrestoIcebergSchema = prestoIcebergSchema(typeManager);

        // Convert to PrestoIcebergSchema
        PrestoIcebergSchema prestoSchema = SchemaConverter.toPrestoSchema(mockSchema, typeManager);

        // Check that the result is not null
        assertNotNull(prestoSchema);

        assertEquals(prestoSchema, expectedPrestoIcebergSchema);
    }

    @Test
    public void testToIcebergSchema()
    {
        // Create a test TypeManager
        TypeManager typeManager = createTestFunctionAndTypeManager();

        // Create a mock Presto Iceberg Schema
        PrestoIcebergSchema prestoSchema = prestoIcebergSchema(typeManager);

        Schema expectedIcebergSchema = schema();

        // Convert Presto Iceberg Schema to Iceberg Schema
        Schema schema = SchemaConverter.toIcebergSchema(prestoSchema);

        // Check that the result is not null
        assertNotNull(schema);

        assertTrue(schema.sameSchema(expectedIcebergSchema));
    }

    protected static PrestoIcebergSchema prestoIcebergSchema(TypeManager typeManager)
    {
        List<PrestoIcebergNestedField> columns = new ArrayList<>(Arrays.asList(
                prestoIcebergNestedField(1, "boolean", typeManager),
                prestoIcebergNestedField(2, "integer", typeManager),
                prestoIcebergNestedField(3, "array", typeManager),
                prestoIcebergNestedField(4, "bigint", typeManager),
                prestoIcebergNestedField(5, "real", typeManager),
                prestoIcebergNestedField(6, "double", typeManager),
                prestoIcebergNestedField(7, "map", typeManager),
                prestoIcebergNestedField(8, "decimal", typeManager),
                prestoIcebergNestedField(9, "varchar", typeManager),
                prestoIcebergNestedField(10, "varbinary", typeManager),
                prestoIcebergNestedField(11, "row", typeManager),
                prestoIcebergNestedField(12, "date", typeManager),
                prestoIcebergNestedField(13, "timestamp", typeManager),
                prestoIcebergNestedField(14, "timestamptz", typeManager)));

        Map<String, Integer> columnNameToIdMapping = getColumnNameToIdMapping();

        return new PrestoIcebergSchema(0, columns, columnNameToIdMapping, null, emptySet());
    }

    private static Map<String, Integer> getColumnNameToIdMapping()
    {
        Map<String, Integer> columnNameToIdMapping = new HashMap<>();
        columnNameToIdMapping.put("boolean", 1);
        columnNameToIdMapping.put("integer", 2);
        columnNameToIdMapping.put("array", 3);
        columnNameToIdMapping.put("bigint", 4);
        columnNameToIdMapping.put("real", 5);
        columnNameToIdMapping.put("double", 6);
        columnNameToIdMapping.put("map", 7);
        columnNameToIdMapping.put("decimal", 8);
        columnNameToIdMapping.put("varchar", 9);
        columnNameToIdMapping.put("varbinary", 10);
        columnNameToIdMapping.put("row", 11);
        columnNameToIdMapping.put("date", 12);
        columnNameToIdMapping.put("timestamp", 13);
        columnNameToIdMapping.put("timestamptz", 14);
        columnNameToIdMapping.put("array.element", 15);
        columnNameToIdMapping.put("map.key", 16);
        columnNameToIdMapping.put("map.value", 17);
        columnNameToIdMapping.put("row.int", 18);
        columnNameToIdMapping.put("row.varchar", 19);

        return columnNameToIdMapping;
    }

    protected static Schema schema()
    {
        List<NestedField> fields = new ArrayList<>(Arrays.asList(
                nestedField(1, "boolean"),
                nestedField(2, "integer"),
                nestedField(3, "array"),
                nestedField(4, "bigint"),
                nestedField(5, "real"),
                nestedField(6, "double"),
                nestedField(7, "map"),
                nestedField(8, "decimal"),
                nestedField(9, "varchar"),
                nestedField(10, "varbinary"),
                nestedField(11, "row"),
                nestedField(12, "date"),
                nestedField(13, "timestamp"),
                nestedField(14, "timestamptz")));

        Type schemaAsStruct = Types.StructType.of(fields);
        AtomicInteger nextFieldId = new AtomicInteger(1);
        schemaAsStruct = TypeUtil.assignFreshIds(schemaAsStruct, nextFieldId::getAndIncrement);

        return new Schema(schemaAsStruct.asStructType().fields());
    }
}
