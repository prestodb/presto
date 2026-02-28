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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Tests for NestedFieldMapper class.
 */
@Test(singleThreaded = true)
public class TestNestedFieldMapper
{
    private NestedFieldMapper mapper;

    @BeforeMethod
    public void setUp()
    {
        mapper = new NestedFieldMapper(5);
    }

    @Test
    public void testSimpleNestedObject()
    {
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> tokenUsage = new HashMap<>();
        tokenUsage.put("type", "object");

        Map<String, Object> properties = new HashMap<>();
        Map<String, Object> totalTokens = new HashMap<>();
        totalTokens.put("type", "long");
        properties.put("total_tokens", totalTokens);

        tokenUsage.put("properties", properties);
        mapping.put("token_usage", tokenUsage);

        Map<String, NestedFieldInfo> result = mapper.discoverNestedFields(mapping);

        assertEquals(result.size(), 2); // parent + leaf
        assertTrue(result.containsKey("token_usage"));
        assertTrue(result.containsKey("token_usage.total_tokens"));
        assertTrue(result.get("token_usage.total_tokens").isLeafField());
    }

    @Test
    public void testMultipleNestedFields()
    {
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

        Map<String, Object> completionTokens = new HashMap<>();
        completionTokens.put("type", "long");
        properties.put("completion_tokens", completionTokens);

        tokenUsage.put("properties", properties);
        mapping.put("token_usage", tokenUsage);

        Map<String, NestedFieldInfo> result = mapper.discoverNestedFields(mapping);

        assertEquals(result.size(), 4); // 1 parent + 3 leaves
        assertTrue(result.containsKey("token_usage.total_tokens"));
        assertTrue(result.containsKey("token_usage.prompt_tokens"));
        assertTrue(result.containsKey("token_usage.completion_tokens"));
    }

    @Test
    public void testDeeplyNestedStructure()
    {
        Map<String, Object> mapping = new HashMap<>();

        // Create a.b.c.d structure
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

        Map<String, NestedFieldInfo> result = mapper.discoverNestedFields(mapping);

        assertTrue(result.containsKey("a.b.c.d"));
        NestedFieldInfo leafField = result.get("a.b.c.d");
        assertEquals(leafField.getNestingLevel(), 3);
        assertTrue(leafField.isLeafField());
    }

    @Test
    public void testMaxDepthLimit()
    {
        NestedFieldMapper limitedMapper = new NestedFieldMapper(2);

        Map<String, Object> mapping = new HashMap<>();

        // Create a.b.c.d structure (depth 3)
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

        Map<String, NestedFieldInfo> result = limitedMapper.discoverNestedFields(mapping);

        // Should stop at depth 2, so a.b.c.d should not be discovered
        assertFalse(result.containsKey("a.b.c.d"));
        assertTrue(result.containsKey("a.b"));
    }

    @Test
    public void testMixedNestedAndRegularFields()
    {
        Map<String, Object> mapping = new HashMap<>();

        // Regular field
        Map<String, Object> id = new HashMap<>();
        id.put("type", "keyword");
        mapping.put("id", id);

        // Nested object
        Map<String, Object> user = new HashMap<>();
        user.put("type", "object");
        Map<String, Object> userProps = new HashMap<>();
        Map<String, Object> name = new HashMap<>();
        name.put("type", "text");
        userProps.put("name", name);
        user.put("properties", userProps);
        mapping.put("user", user);

        Map<String, NestedFieldInfo> result = mapper.discoverNestedFields(mapping);

        assertTrue(result.containsKey("id"));
        assertTrue(result.containsKey("user"));
        assertTrue(result.containsKey("user.name"));
        assertEquals(result.get("id").getNestingLevel(), 0);
        assertEquals(result.get("user.name").getNestingLevel(), 1);
    }

    @Test
    public void testVariousDataTypes()
    {
        Map<String, Object> mapping = new HashMap<>();

        Map<String, Object> longField = new HashMap<>();
        longField.put("type", "long");
        mapping.put("count", longField);

        Map<String, Object> doubleField = new HashMap<>();
        doubleField.put("type", "double");
        mapping.put("price", doubleField);

        Map<String, Object> boolField = new HashMap<>();
        boolField.put("type", "boolean");
        mapping.put("active", boolField);

        Map<String, Object> textField = new HashMap<>();
        textField.put("type", "text");
        mapping.put("description", textField);

        Map<String, Object> keywordField = new HashMap<>();
        keywordField.put("type", "keyword");
        mapping.put("id", keywordField);

        Map<String, NestedFieldInfo> result = mapper.discoverNestedFields(mapping);

        assertEquals(result.size(), 5);
        assertTrue(result.containsKey("count"));
        assertTrue(result.containsKey("price"));
        assertTrue(result.containsKey("active"));
        assertTrue(result.containsKey("description"));
        assertTrue(result.containsKey("id"));
    }

    @Test
    public void testEmptyMapping()
    {
        Map<String, Object> mapping = new HashMap<>();
        Map<String, NestedFieldInfo> result = mapper.discoverNestedFields(mapping);

        assertEquals(result.size(), 0);
    }

    @Test
    public void testObjectWithoutProperties()
    {
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> emptyObject = new HashMap<>();
        emptyObject.put("type", "object");
        // No properties defined
        mapping.put("empty_obj", emptyObject);

        Map<String, NestedFieldInfo> result = mapper.discoverNestedFields(mapping);

        // Should still create entry for the parent object
        assertTrue(result.containsKey("empty_obj"));
        assertFalse(result.get("empty_obj").isLeafField());
    }

    @Test
    public void testNestedArrayOfObjects()
    {
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> items = new HashMap<>();
        items.put("type", "nested");

        Map<String, Object> properties = new HashMap<>();
        Map<String, Object> name = new HashMap<>();
        name.put("type", "text");
        properties.put("name", name);

        Map<String, Object> quantity = new HashMap<>();
        quantity.put("type", "integer");
        properties.put("quantity", quantity);

        items.put("properties", properties);
        mapping.put("items", items);

        Map<String, NestedFieldInfo> result = mapper.discoverNestedFields(mapping);

        assertTrue(result.containsKey("items"));
        assertTrue(result.containsKey("items.name"));
        assertTrue(result.containsKey("items.quantity"));
        assertFalse(result.get("items").isLeafField());
        assertTrue(result.get("items.name").isLeafField());
    }

    @Test
    public void testComplexRealWorldStructure()
    {
        Map<String, Object> mapping = new HashMap<>();

        // Metadata object
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

        // Token usage object
        Map<String, Object> tokenUsage = new HashMap<>();
        tokenUsage.put("type", "object");
        Map<String, Object> tokenProps = new HashMap<>();

        Map<String, Object> total = new HashMap<>();
        total.put("type", "long");
        tokenProps.put("total_tokens", total);

        tokenUsage.put("properties", tokenProps);
        mapping.put("token_usage", tokenUsage);

        Map<String, NestedFieldInfo> result = mapper.discoverNestedFields(mapping);

        assertTrue(result.containsKey("metadata.id"));
        assertTrue(result.containsKey("metadata.timestamp"));
        assertTrue(result.containsKey("token_usage.total_tokens"));
    }

    @Test
    public void testGetLeafFieldsOnly()
    {
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> user = new HashMap<>();
        user.put("type", "object");

        Map<String, Object> properties = new HashMap<>();
        Map<String, Object> name = new HashMap<>();
        name.put("type", "text");
        properties.put("name", name);

        user.put("properties", properties);
        mapping.put("user", user);

        Map<String, NestedFieldInfo> allFields = mapper.discoverNestedFields(mapping);
        Map<String, NestedFieldInfo> leafFields = mapper.getLeafFieldsOnly(allFields);

        // Should only contain user.name, not user
        assertEquals(leafFields.size(), 1);
        assertTrue(leafFields.containsKey("user.name"));
        assertFalse(leafFields.containsKey("user"));
    }

    @Test
    public void testGetMaxDepth()
    {
        NestedFieldMapper mapper1 = new NestedFieldMapper(5);
        assertEquals(mapper1.getMaxDepth(), 5);

        NestedFieldMapper mapper2 = new NestedFieldMapper(10);
        assertEquals(mapper2.getMaxDepth(), 10);
    }

    @Test
    public void testFieldPathConstruction()
    {
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> a = new HashMap<>();
        a.put("type", "object");
        Map<String, Object> aProps = new HashMap<>();

        Map<String, Object> b = new HashMap<>();
        b.put("type", "object");
        Map<String, Object> bProps = new HashMap<>();

        Map<String, Object> c = new HashMap<>();
        c.put("type", "text");

        bProps.put("c", c);
        b.put("properties", bProps);
        aProps.put("b", b);
        a.put("properties", aProps);
        mapping.put("a", a);

        Map<String, NestedFieldInfo> result = mapper.discoverNestedFields(mapping);

        NestedFieldInfo field = result.get("a.b.c");
        assertNotNull(field);
        assertEquals(field.getFieldPath(), "a.b.c");
        assertEquals(field.getParentPath(), "a.b");
        assertEquals(field.getFieldName(), "c");
    }

    @Test
    public void testNestingLevelCalculation()
    {
        Map<String, Object> mapping = new HashMap<>();

        // Root level
        Map<String, Object> root = new HashMap<>();
        root.put("type", "text");
        mapping.put("root", root);

        // Level 1
        Map<String, Object> level1 = new HashMap<>();
        level1.put("type", "object");
        Map<String, Object> l1Props = new HashMap<>();
        Map<String, Object> field1 = new HashMap<>();
        field1.put("type", "text");
        l1Props.put("field", field1);
        level1.put("properties", l1Props);
        mapping.put("level1", level1);

        Map<String, NestedFieldInfo> result = mapper.discoverNestedFields(mapping);

        assertEquals(result.get("root").getNestingLevel(), 0);
        assertEquals(result.get("level1").getNestingLevel(), 0);
        assertEquals(result.get("level1.field").getNestingLevel(), 1);
    }

    @Test
    public void testDefaultTypeHandling()
    {
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> field = new HashMap<>();
        // No type specified - should default to "object"
        Map<String, Object> properties = new HashMap<>();
        Map<String, Object> subfield = new HashMap<>();
        subfield.put("type", "text");
        properties.put("subfield", subfield);
        field.put("properties", properties);
        mapping.put("field", field);

        Map<String, NestedFieldInfo> result = mapper.discoverNestedFields(mapping);

        assertTrue(result.containsKey("field"));
        assertTrue(result.containsKey("field.subfield"));
        assertFalse(result.get("field").isLeafField());
    }
}
