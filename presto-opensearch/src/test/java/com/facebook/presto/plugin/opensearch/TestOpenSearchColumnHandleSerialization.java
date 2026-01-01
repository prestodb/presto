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

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Tests for OpenSearchColumnHandle computed properties.
 * Specifically tests that isNestedField() is correctly computed from fieldPathList,
 * which ensures the property is preserved even when column handles are serialized
 * and deserialized between query coordinator and worker threads.
 */
public class TestOpenSearchColumnHandleSerialization
{
    /**
     * Test that isNestedField() returns true for nested fields (fieldPathList.size() > 1).
     * This is the core fix for the bug where nested fields were showing NULL values.
     */
    @Test
    public void testNestedFieldComputedProperty()
    {
        // Create nested field column handle with 2-level path
        List<String> fieldPath = Arrays.asList("token_usage", "total_tokens");
        OpenSearchColumnHandle column = new OpenSearchColumnHandle(
                "token_usage.total_tokens",
                BIGINT,
                "long",
                false,
                0,
                false,  // This parameter is ignored; isNestedField is computed
                "token_usage",
                fieldPath,
                "$.token_usage.total_tokens");

        // Verify isNestedField is computed correctly
        assertTrue(column.isNestedField(), "Should be nested field when fieldPathList.size() > 1");
        assertEquals(column.getFieldPathList().size(), 2);
        assertEquals(column.getFieldPathList(), fieldPath);
    }

    /**
     * Test that isNestedField() returns false for top-level fields (fieldPathList.size() == 1).
     */
    @Test
    public void testTopLevelFieldComputedProperty()
    {
        // Create top-level column handle
        List<String> fieldPath = Arrays.asList("user_id");
        OpenSearchColumnHandle column = new OpenSearchColumnHandle(
                "user_id",
                VARCHAR,
                "keyword",
                false,
                0,
                true,  // This parameter is ignored; isNestedField is computed
                "",
                fieldPath,
                "$.user_id");

        // Verify isNestedField is computed correctly (should be false)
        assertFalse(column.isNestedField(), "Should not be nested field when fieldPathList.size() == 1");
        assertEquals(column.getFieldPathList().size(), 1);
    }

    /**
     * Test deeply nested fields (3+ levels).
     */
    @Test
    public void testDeeplyNestedFieldComputedProperty()
    {
        // Create deeply nested field: reliability_scores.answer_relevance.score
        List<String> fieldPath = Arrays.asList("reliability_scores", "answer_relevance", "score");
        OpenSearchColumnHandle column = new OpenSearchColumnHandle(
                "reliability_scores.answer_relevance.score",
                BIGINT,
                "long",
                false,
                0,
                false,  // This parameter is ignored
                "reliability_scores.answer_relevance",
                fieldPath,
                "$.reliability_scores.answer_relevance.score");

        // Verify deep nesting is detected
        assertTrue(column.isNestedField(), "Should be nested field for 3-level path");
        assertEquals(column.getFieldPathList().size(), 3);
        assertEquals(column.getFieldPathList().get(0), "reliability_scores");
        assertEquals(column.getFieldPathList().get(1), "answer_relevance");
        assertEquals(column.getFieldPathList().get(2), "score");
    }

    /**
     * Test that constructor parameter for isNestedField is ignored.
     * The value should always be computed from fieldPathList.
     */
    @Test
    public void testConstructorParameterIgnored()
    {
        // Pass true for isNestedField but single-element path
        List<String> singlePath = Arrays.asList("simple_field");
        OpenSearchColumnHandle singleLevel = new OpenSearchColumnHandle(
                "simple_field",
                VARCHAR,
                "keyword",
                false,
                0,
                true,  // Try to force it to be nested
                "",
                singlePath,
                "$.simple_field");

        // Should still be false because fieldPathList.size() == 1
        assertFalse(singleLevel.isNestedField(), "Constructor parameter should be ignored");

        // Pass false for isNestedField but multi-element path
        List<String> multiPath = Arrays.asList("parent", "child");
        OpenSearchColumnHandle multiLevel = new OpenSearchColumnHandle(
                "parent.child",
                VARCHAR,
                "text",
                false,
                0,
                false,  // Try to force it to be non-nested
                "parent",
                multiPath,
                "$.parent.child");

        // Should still be true because fieldPathList.size() > 1
        assertTrue(multiLevel.isNestedField(), "Constructor parameter should be ignored");
    }

    /**
     * Test that fieldPathList is never null and is immutable.
     */
    @Test
    public void testFieldPathListProperties()
    {
        List<String> fieldPath = Arrays.asList("user", "profile", "email");
        OpenSearchColumnHandle column = new OpenSearchColumnHandle(
                "user.profile.email",
                VARCHAR,
                "keyword",
                false,
                0,
                true,
                "user.profile",
                fieldPath,
                "$.user.profile.email");

        assertNotNull(column.getFieldPathList(), "fieldPathList should never be null");
        assertEquals(column.getFieldPathList().size(), 3);
        assertTrue(column.isNestedField());

        // Verify list is immutable
        try {
            column.getFieldPathList().add("extra");
            throw new AssertionError("fieldPathList should be immutable");
        }
        catch (UnsupportedOperationException e) {
            // Expected
        }
    }

    /**
     * Test empty fieldPathList results in non-nested field.
     */
    @Test
    public void testEmptyFieldPathList()
    {
        OpenSearchColumnHandle column = new OpenSearchColumnHandle(
                "field",
                VARCHAR,
                "keyword",
                false,
                0,
                true,  // Try to force nested
                "",
                Collections.emptyList(),
                "$.field");

        assertFalse(column.isNestedField(), "Empty fieldPathList should result in non-nested field");
        assertEquals(column.getFieldPathList().size(), 0);
    }

    /**
     * Test null fieldPathList is converted to empty list.
     */
    @Test
    public void testNullFieldPathList()
    {
        OpenSearchColumnHandle column = new OpenSearchColumnHandle(
                "field",
                VARCHAR,
                "keyword",
                false,
                0,
                true,  // Try to force nested
                "",
                null,  // Pass null
                "$.field");

        assertNotNull(column.getFieldPathList(), "Null fieldPathList should be converted to empty list");
        assertFalse(column.isNestedField(), "Null fieldPathList should result in non-nested field");
        assertEquals(column.getFieldPathList().size(), 0);
    }

    /**
     * Test parent object column handle (depth 0).
     */
    @Test
    public void testParentObjectColumn()
    {
        List<String> fieldPath = Arrays.asList("token_usage");
        OpenSearchColumnHandle column = new OpenSearchColumnHandle(
                "token_usage",
                VARCHAR,  // Parent objects are VARCHAR (JSON strings)
                "object",
                false,
                0,
                true,  // Try to force nested
                "",
                fieldPath,
                "$.token_usage");

        // Parent at depth 0 should not be nested
        assertFalse(column.isNestedField(), "Parent object at depth 0 should not be nested");
        assertEquals(column.getFieldPathList().size(), 1);
    }
}
