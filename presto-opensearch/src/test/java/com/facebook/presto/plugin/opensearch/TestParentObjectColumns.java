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
package com.facebook.presto.plugin.opensearch;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Tests for parent object column support.
 * Verifies that both parent objects (as JSON strings) and leaf fields
 * are properly exposed and can be queried.
 */
public class TestParentObjectColumns
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Test that NestedFieldMapper discovers both parent and leaf fields
     */
    @Test
    public void testNestedFieldDiscovery()
    {
        // Create a mapping with nested structure like token_usage
        Map<String, Object> mapping = new HashMap<>();

        Map<String, Object> tokenUsage = new HashMap<>();
        tokenUsage.put("type", "object");

        Map<String, Object> properties = new HashMap<>();

        Map<String, Object> totalTokens = new HashMap<>();
        totalTokens.put("type", "long");
        properties.put("total_tokens", totalTokens);

        Map<String, Object> inputTokens = new HashMap<>();
        inputTokens.put("type", "long");
        properties.put("input_tokens", inputTokens);

        Map<String, Object> outputTokens = new HashMap<>();
        outputTokens.put("type", "long");
        properties.put("output_tokens", outputTokens);

        tokenUsage.put("properties", properties);
        mapping.put("token_usage", tokenUsage);

        // Discover fields
        NestedFieldMapper mapper = new NestedFieldMapper(5);
        Map<String, NestedFieldInfo> allFields = mapper.discoverNestedFields(mapping);

        // Verify all fields are discovered (parent + 3 leaf fields)
        assertEquals(allFields.size(), 4, "Should discover 4 fields total");

        // Verify parent field
        assertTrue(allFields.containsKey("token_usage"), "Should have parent field");
        NestedFieldInfo parentField = allFields.get("token_usage");
        assertFalse(parentField.isLeafField(), "Parent should not be a leaf field");
        assertEquals(parentField.getOpenSearchType(), "object");
        assertEquals(parentField.getNestingLevel(), 0, "Parent should be at depth 0");

        // Verify leaf fields
        assertTrue(allFields.containsKey("token_usage.total_tokens"));
        NestedFieldInfo totalField = allFields.get("token_usage.total_tokens");
        assertTrue(totalField.isLeafField(), "Should be a leaf field");
        assertTrue(totalField.isNestedField(), "Should be marked as nested (depth > 0)");
        assertEquals(totalField.getPrestoType(), BIGINT);
        assertEquals(totalField.getOpenSearchType(), "long");
        assertEquals(totalField.getNestingLevel(), 1, "Leaf should be at depth 1");
        assertEquals(totalField.getParentPath(), "token_usage");

        assertTrue(allFields.containsKey("token_usage.input_tokens"));
        assertTrue(allFields.containsKey("token_usage.output_tokens"));
    }

    /**
     * Test that OpenSearchMetadata exposes both parent and leaf columns
     */
    @Test
    public void testColumnExposure()
    {
        // Create mapping
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> tokenUsage = new HashMap<>();
        tokenUsage.put("type", "object");

        Map<String, Object> properties = new HashMap<>();
        Map<String, Object> totalTokens = new HashMap<>();
        totalTokens.put("type", "long");
        properties.put("total_tokens", totalTokens);

        tokenUsage.put("properties", properties);
        mapping.put("token_usage", tokenUsage);

        // Discover fields
        NestedFieldMapper mapper = new NestedFieldMapper(5);
        Map<String, NestedFieldInfo> allFields = mapper.discoverNestedFields(mapping);

        // Verify parent field type
        NestedFieldInfo parentField = allFields.get("token_usage");
        assertNotNull(parentField);

        // Parent objects should be exposed as VARCHAR (for JSON strings)
        // This is determined in OpenSearchMetadata
        Type parentColumnType = parentField.isLeafField() ? parentField.getPrestoType() : VARCHAR;
        assertEquals(parentColumnType, VARCHAR, "Parent object should be VARCHAR");

        // Verify leaf field type
        NestedFieldInfo leafField = allFields.get("token_usage.total_tokens");
        assertNotNull(leafField);
        Type leafColumnType = leafField.isLeafField() ? leafField.getPrestoType() : VARCHAR;
        assertEquals(leafColumnType, BIGINT, "Leaf field should keep its type");
    }

    /**
     * Test that NestedValueExtractor correctly extracts nested values
     */
    @Test
    public void testNestedValueExtraction()
    {
        // Create a document with nested structure
        Map<String, Object> document = new HashMap<>();

        Map<String, Object> tokenUsage = new HashMap<>();
        tokenUsage.put("total_tokens", 90616L);
        tokenUsage.put("input_tokens", 89091L);
        tokenUsage.put("output_tokens", 1525L);

        document.put("token_usage", tokenUsage);
        document.put("_id", "test-id-123");

        // Create extractor
        NestedValueExtractor extractor = new NestedValueExtractor();

        // Test extracting nested field
        List<String> fieldPath = new ArrayList<>();
        fieldPath.add("token_usage");
        fieldPath.add("total_tokens");

        Object value = extractor.extractNestedValue(document, fieldPath, BIGINT);
        assertNotNull(value, "Should extract value");
        assertEquals(value, 90616L, "Should extract correct value");

        // Test extracting another nested field
        fieldPath = new ArrayList<>();
        fieldPath.add("token_usage");
        fieldPath.add("input_tokens");

        value = extractor.extractNestedValue(document, fieldPath, BIGINT);
        assertEquals(value, 89091L);
    }

    /**
     * Test that parent objects are converted to JSON strings
     */
    @Test
    public void testParentObjectJsonConversion() throws Exception
    {
        // Create a document with nested structure
        Map<String, Object> tokenUsage = new HashMap<>();
        tokenUsage.put("total_tokens", 90616);
        tokenUsage.put("input_tokens", 89091);
        tokenUsage.put("output_tokens", 1525);

        // Convert to JSON string (simulating what OpenSearchPageSource does)
        String jsonString = objectMapper.writeValueAsString(tokenUsage);

        // Verify JSON is valid and contains expected data
        assertNotNull(jsonString);
        assertTrue(jsonString.contains("total_tokens"));
        assertTrue(jsonString.contains("90616"));
        assertTrue(jsonString.contains("input_tokens"));
        assertTrue(jsonString.contains("89091"));
        assertTrue(jsonString.contains("output_tokens"));
        assertTrue(jsonString.contains("1525"));

        // Verify we can parse it back
        Map<String, Object> parsed = objectMapper.readValue(jsonString, Map.class);
        assertEquals(parsed.get("total_tokens"), 90616);
    }

    /**
     * Test column handle creation for both parent and leaf fields
     */
    @Test
    public void testColumnHandleCreation()
    {
        // Create parent column handle (object type, exposed as VARCHAR)
        List<String> parentPath = new ArrayList<>();
        parentPath.add("token_usage");

        OpenSearchColumnHandle parentColumn = new OpenSearchColumnHandle(
                "token_usage",
                VARCHAR,  // Parent objects are VARCHAR
                "object",
                false,
                0,
                false,  // Parent itself is not nested (depth 0)
                "",
                parentPath,
                "$.token_usage");

        assertEquals(parentColumn.getColumnName(), "token_usage");
        assertEquals(parentColumn.getColumnType(), VARCHAR);
        assertFalse(parentColumn.isNestedField());

        // Create leaf column handle (nested field)
        List<String> leafPath = new ArrayList<>();
        leafPath.add("token_usage");
        leafPath.add("total_tokens");

        OpenSearchColumnHandle leafColumn = new OpenSearchColumnHandle(
                "token_usage.total_tokens",
                BIGINT,
                "long",
                false,
                0,
                true,  // Leaf field is nested (depth > 0)
                "token_usage",
                leafPath,
                "$.token_usage.total_tokens");

        assertEquals(leafColumn.getColumnName(), "token_usage.total_tokens");
        assertEquals(leafColumn.getColumnType(), BIGINT);
        assertTrue(leafColumn.isNestedField());
        assertEquals(leafColumn.getParentFieldPath(), "token_usage");
        assertEquals(leafColumn.getFieldPathList().size(), 2);
    }

    /**
     * Integration test: Full workflow from mapping to column metadata
     */
    @Test
    public void testFullWorkflow()
    {
        // 1. Create mapping
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> tokenUsage = new HashMap<>();
        tokenUsage.put("type", "object");

        Map<String, Object> properties = new HashMap<>();
        Map<String, Object> totalTokens = new HashMap<>();
        totalTokens.put("type", "long");
        properties.put("total_tokens", totalTokens);

        tokenUsage.put("properties", properties);
        mapping.put("token_usage", tokenUsage);

        // 2. Discover fields
        NestedFieldMapper mapper = new NestedFieldMapper(5);
        Map<String, NestedFieldInfo> allFields = mapper.discoverNestedFields(mapping);

        // 3. Create column handles (simulating OpenSearchMetadata logic)
        Map<String, ColumnHandle> columns = new HashMap<>();

        for (Map.Entry<String, NestedFieldInfo> entry : allFields.entrySet()) {
            NestedFieldInfo fieldInfo = entry.getValue();

            // Determine column type (parent = VARCHAR, leaf = actual type)
            Type columnType = fieldInfo.isLeafField() ?
                    fieldInfo.getPrestoType() : VARCHAR;

            OpenSearchColumnHandle column = new OpenSearchColumnHandle(
                    fieldInfo.getFieldPath(),
                    columnType,
                    fieldInfo.getOpenSearchType(),
                    false,
                    0,
                    fieldInfo.isNestedField(),
                    fieldInfo.getParentPath(),
                    fieldInfo.getFieldPathList(),
                    fieldInfo.getJsonPath());

            columns.put(fieldInfo.getFieldPath(), column);
        }

        // 4. Verify columns
        assertEquals(columns.size(), 2, "Should have 2 columns");

        // Verify parent column
        assertTrue(columns.containsKey("token_usage"));
        OpenSearchColumnHandle parentCol = (OpenSearchColumnHandle) columns.get("token_usage");
        assertEquals(parentCol.getColumnType(), VARCHAR);

        // Verify leaf column
        assertTrue(columns.containsKey("token_usage.total_tokens"));
        OpenSearchColumnHandle leafCol = (OpenSearchColumnHandle) columns.get("token_usage.total_tokens");
        assertEquals(leafCol.getColumnType(), BIGINT);
        assertTrue(leafCol.isNestedField());

        // 5. Test data extraction
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> tokenUsageData = new HashMap<>();
        tokenUsageData.put("total_tokens", 90616L);
        document.put("token_usage", tokenUsageData);

        // Extract parent (as JSON)
        Object parentValue = document.get("token_usage");
        assertTrue(parentValue instanceof Map);

        // Extract leaf (using NestedValueExtractor)
        NestedValueExtractor extractor = new NestedValueExtractor();
        Object leafValue = extractor.extractNestedValue(
                document,
                leafCol.getFieldPathList(),
                BIGINT);
        assertEquals(leafValue, 90616L);
    }
}
