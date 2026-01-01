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

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.VarcharType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Integration tests for nested field functionality.
 * Tests the complete flow from mapping discovery to value extraction.
 */
@Test(singleThreaded = true)
public class TestNestedFieldIntegration
{
    private NestedFieldMapper mapper;
    private NestedValueExtractor extractor;

    @BeforeMethod
    public void setUp()
    {
        mapper = new NestedFieldMapper(5);
        extractor = new NestedValueExtractor();
    }

    @Test
    public void testCompleteTokenUsageFlow()
    {
        // Step 1: Define mapping
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> tokenUsage = new HashMap<>();
        tokenUsage.put("type", "object");

        Map<String, Object> properties = new HashMap<>();
        Map<String, Object> totalTokens = new HashMap<>();
        totalTokens.put("type", "long");
        properties.put("total_tokens", totalTokens);

        Map<String, Object> promptTokens = new HashMap<>();
        promptTokens.put("type", "long");
        properties.put("prompt_tokens", promptTokens);

        tokenUsage.put("properties", properties);
        mapping.put("token_usage", tokenUsage);

        // Step 2: Discover fields
        Map<String, NestedFieldInfo> fields = mapper.discoverNestedFields(mapping);

        assertTrue(fields.containsKey("token_usage.total_tokens"));
        assertTrue(fields.containsKey("token_usage.prompt_tokens"));

        NestedFieldInfo totalTokensInfo = fields.get("token_usage.total_tokens");
        assertNotNull(totalTokensInfo);
        assertEquals(totalTokensInfo.getFieldPath(), "token_usage.total_tokens");
        assertEquals(totalTokensInfo.getPrestoType(), BigintType.BIGINT);

        // Step 3: Create document
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> tokenUsageData = new HashMap<>();
        tokenUsageData.put("total_tokens", 90616L);
        tokenUsageData.put("prompt_tokens", 45308L);
        document.put("token_usage", tokenUsageData);

        // Step 4: Extract values
        Object totalValue = extractor.extractNestedValue(
                document,
                Arrays.asList("token_usage", "total_tokens"),
                BigintType.BIGINT);

        Object promptValue = extractor.extractNestedValue(
                document,
                Arrays.asList("token_usage", "prompt_tokens"),
                BigintType.BIGINT);

        assertEquals(totalValue, 90616L);
        assertEquals(promptValue, 45308L);
    }

    @Test
    public void testMultipleNestedObjects()
    {
        // Mapping with multiple nested objects
        Map<String, Object> mapping = new HashMap<>();

        // User object
        Map<String, Object> user = new HashMap<>();
        user.put("type", "object");
        Map<String, Object> userProps = new HashMap<>();
        Map<String, Object> userName = new HashMap<>();
        userName.put("type", "text");
        userProps.put("name", userName);
        user.put("properties", userProps);
        mapping.put("user", user);

        // Metadata object
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("type", "object");
        Map<String, Object> metaProps = new HashMap<>();
        Map<String, Object> metaId = new HashMap<>();
        metaId.put("type", "keyword");
        metaProps.put("id", metaId);
        metadata.put("properties", metaProps);
        mapping.put("metadata", metadata);

        // Discover fields
        Map<String, NestedFieldInfo> fields = mapper.discoverNestedFields(mapping);

        assertTrue(fields.containsKey("user.name"));
        assertTrue(fields.containsKey("metadata.id"));

        // Create document
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> userData = new HashMap<>();
        userData.put("name", "John Doe");
        document.put("user", userData);

        Map<String, Object> metadataData = new HashMap<>();
        metadataData.put("id", "doc123");
        document.put("metadata", metadataData);

        // Extract values
        Object userNameValue = extractor.extractNestedValue(
                document,
                Arrays.asList("user", "name"),
                VarcharType.VARCHAR);

        Object metaIdValue = extractor.extractNestedValue(
                document,
                Arrays.asList("metadata", "id"),
                VarcharType.VARCHAR);

        assertEquals(userNameValue, "John Doe");
        assertEquals(metaIdValue, "doc123");
    }

    @Test
    public void testDeeplyNestedStructure()
    {
        // Create a.b.c.d mapping
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> a = new HashMap<>();
        a.put("type", "object");
        Map<String, Object> aProps = new HashMap<>();

        Map<String, Object> b = new HashMap<>();
        b.put("type", "object");
        Map<String, Object> bProps = new HashMap<>();

        Map<String, Object> c = new HashMap<>();
        c.put("type", "object");
        Map<String, Object> cProps = new HashMap<>();

        Map<String, Object> d = new HashMap<>();
        d.put("type", "long");

        cProps.put("d", d);
        c.put("properties", cProps);
        bProps.put("c", c);
        b.put("properties", bProps);
        aProps.put("b", b);
        a.put("properties", aProps);
        mapping.put("a", a);

        // Discover
        Map<String, NestedFieldInfo> fields = mapper.discoverNestedFields(mapping);
        assertTrue(fields.containsKey("a.b.c.d"));

        // Create document
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> aData = new HashMap<>();
        Map<String, Object> bData = new HashMap<>();
        Map<String, Object> cData = new HashMap<>();
        cData.put("d", 42L);
        bData.put("c", cData);
        aData.put("b", bData);
        document.put("a", aData);

        // Extract
        Object value = extractor.extractNestedValue(
                document,
                Arrays.asList("a", "b", "c", "d"),
                BigintType.BIGINT);

        assertEquals(value, 42L);
    }

    @Test
    public void testMissingFieldHandling()
    {
        // Mapping
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> user = new HashMap<>();
        user.put("type", "object");
        Map<String, Object> userProps = new HashMap<>();
        Map<String, Object> name = new HashMap<>();
        name.put("type", "text");
        userProps.put("name", name);
        user.put("properties", userProps);
        mapping.put("user", user);

        // Discover
        Map<String, NestedFieldInfo> fields = mapper.discoverNestedFields(mapping);
        assertTrue(fields.containsKey("user.name"));

        // Document without the nested field
        Map<String, Object> document = new HashMap<>();
        document.put("other_field", "value");

        // Extract - should return null
        Object value = extractor.extractNestedValue(
                document,
                Arrays.asList("user", "name"),
                VarcharType.VARCHAR);

        assertNull(value);
    }

    @Test
    public void testMissingParentHandling()
    {
        // Mapping
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> user = new HashMap<>();
        user.put("type", "object");
        Map<String, Object> userProps = new HashMap<>();
        Map<String, Object> name = new HashMap<>();
        name.put("type", "text");
        userProps.put("name", name);
        user.put("properties", userProps);
        mapping.put("user", user);

        // Discover
        Map<String, NestedFieldInfo> fields = mapper.discoverNestedFields(mapping);
        assertTrue(fields.containsKey("user.name"));

        // Document with user but missing name
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> userData = new HashMap<>();
        userData.put("email", "john@example.com");
        document.put("user", userData);

        // Extract - should return null
        Object value = extractor.extractNestedValue(
                document,
                Arrays.asList("user", "name"),
                VarcharType.VARCHAR);

        assertNull(value);
    }

    @Test
    public void testRealWorldAnalyticsDocument()
    {
        // Complex analytics mapping
        Map<String, Object> mapping = new HashMap<>();

        // Metadata
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("type", "object");
        Map<String, Object> metaProps = new HashMap<>();
        Map<String, Object> id = new HashMap<>();
        id.put("type", "keyword");
        metaProps.put("id", id);
        Map<String, Object> timestamp = new HashMap<>();
        timestamp.put("type", "date");
        metaProps.put("timestamp", timestamp);
        metadata.put("properties", metaProps);
        mapping.put("metadata", metadata);

        // Token usage
        Map<String, Object> tokenUsage = new HashMap<>();
        tokenUsage.put("type", "object");
        Map<String, Object> tokenProps = new HashMap<>();
        Map<String, Object> total = new HashMap<>();
        total.put("type", "long");
        tokenProps.put("total_tokens", total);
        tokenUsage.put("properties", tokenProps);
        mapping.put("token_usage", tokenUsage);

        // Model info
        Map<String, Object> model = new HashMap<>();
        model.put("type", "object");
        Map<String, Object> modelProps = new HashMap<>();
        Map<String, Object> modelName = new HashMap<>();
        modelName.put("type", "keyword");
        modelProps.put("name", modelName);
        model.put("properties", modelProps);
        mapping.put("model", model);

        // Discover all fields
        Map<String, NestedFieldInfo> fields = mapper.discoverNestedFields(mapping);

        assertTrue(fields.containsKey("metadata.id"));
        assertTrue(fields.containsKey("token_usage.total_tokens"));
        assertTrue(fields.containsKey("model.name"));

        // Create document
        Map<String, Object> document = new HashMap<>();

        Map<String, Object> metaData = new HashMap<>();
        metaData.put("id", "doc123");
        metaData.put("timestamp", "2024-01-01T00:00:00Z");
        document.put("metadata", metaData);

        Map<String, Object> tokenData = new HashMap<>();
        tokenData.put("total_tokens", 90616L);
        document.put("token_usage", tokenData);

        Map<String, Object> modelData = new HashMap<>();
        modelData.put("name", "gpt-4");
        document.put("model", modelData);

        // Extract all values
        Object metaId = extractor.extractNestedValue(
                document,
                Arrays.asList("metadata", "id"),
                VarcharType.VARCHAR);

        Object tokens = extractor.extractNestedValue(
                document,
                Arrays.asList("token_usage", "total_tokens"),
                BigintType.BIGINT);

        Object modelNameValue = extractor.extractNestedValue(
                document,
                Arrays.asList("model", "name"),
                VarcharType.VARCHAR);

        assertEquals(metaId, "doc123");
        assertEquals(tokens, 90616L);
        assertEquals(modelNameValue, "gpt-4");
    }

    @Test
    public void testFieldInfoMetadata()
    {
        // Mapping
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> user = new HashMap<>();
        user.put("type", "object");
        Map<String, Object> userProps = new HashMap<>();
        Map<String, Object> age = new HashMap<>();
        age.put("type", "integer");
        userProps.put("age", age);
        user.put("properties", userProps);
        mapping.put("user", user);

        // Discover
        Map<String, NestedFieldInfo> fields = mapper.discoverNestedFields(mapping);

        NestedFieldInfo ageInfo = fields.get("user.age");
        assertNotNull(ageInfo);

        // Verify metadata
        assertEquals(ageInfo.getFieldPath(), "user.age");
        assertEquals(ageInfo.getParentPath(), "user");
        assertEquals(ageInfo.getFieldName(), "age");
        assertEquals(ageInfo.getNestingLevel(), 1);
        assertTrue(ageInfo.isNestedField());
        assertTrue(ageInfo.isLeafField());
        assertEquals(ageInfo.getFieldPathList(), Arrays.asList("user", "age"));
        assertEquals(ageInfo.getJsonPath(), "$.user.age");
    }

    @Test
    public void testLeafFieldFiltering()
    {
        // Mapping with parent and leaf fields
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> user = new HashMap<>();
        user.put("type", "object");
        Map<String, Object> userProps = new HashMap<>();
        Map<String, Object> name = new HashMap<>();
        name.put("type", "text");
        userProps.put("name", name);
        user.put("properties", userProps);
        mapping.put("user", user);

        // Discover all fields
        Map<String, NestedFieldInfo> allFields = mapper.discoverNestedFields(mapping);
        assertEquals(allFields.size(), 2); // user + user.name

        // Get only leaf fields
        Map<String, NestedFieldInfo> leafFields = mapper.getLeafFieldsOnly(allFields);
        assertEquals(leafFields.size(), 1); // only user.name
        assertTrue(leafFields.containsKey("user.name"));
    }

    @Test
    public void testEmptyDocumentHandling()
    {
        // Mapping
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> user = new HashMap<>();
        user.put("type", "object");
        Map<String, Object> userProps = new HashMap<>();
        Map<String, Object> name = new HashMap<>();
        name.put("type", "text");
        userProps.put("name", name);
        user.put("properties", userProps);
        mapping.put("user", user);

        // Discover
        Map<String, NestedFieldInfo> fields = mapper.discoverNestedFields(mapping);
        assertTrue(fields.containsKey("user.name"));

        // Empty document
        Map<String, Object> document = new HashMap<>();

        // Extract - should return null
        Object value = extractor.extractNestedValue(
                document,
                Arrays.asList("user", "name"),
                VarcharType.VARCHAR);

        assertNull(value);
    }
}
