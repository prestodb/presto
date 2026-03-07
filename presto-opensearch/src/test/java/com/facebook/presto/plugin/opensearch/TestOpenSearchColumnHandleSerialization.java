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

import com.facebook.presto.common.type.ArrayType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Tests for OpenSearchColumnHandle to ensure proper handling of complex field names.
 * This addresses the bug where field names like "cache_creation_input_tokens"
 * could cause issues during query execution.
 */
public class TestOpenSearchColumnHandleSerialization
{
    @Test
    public void testSimpleColumnHandleCreation()
    {
        OpenSearchColumnHandle handle = new OpenSearchColumnHandle(
                "user_id",
                VARCHAR,
                "keyword");

        assertEquals(handle.getColumnName(), "user_id");
        assertEquals(handle.getColumnType(), VARCHAR);
        assertEquals(handle.getOpenSearchType(), "keyword");
        assertFalse(handle.isVector());
        assertFalse(handle.isNestedField());
    }

    @Test
    public void testProblematicFieldNamesCreation()
    {
        // Test field names that caused the original bug
        String[] problematicFieldNames = {
            "cache_creation_input_tokens",
            "cache_creation_output_tokens",
            "cache_read_input_tokens",
            "total_input_tokens",
            "prompt_tokens_used",
            "completion_tokens_generated",
            "model_response_time_ms"
        };

        for (String fieldName : problematicFieldNames) {
            OpenSearchColumnHandle handle = new OpenSearchColumnHandle(
                    fieldName,
                    BIGINT,
                    "long");

            assertEquals(handle.getColumnName(), fieldName,
                    "Field name mismatch for: " + fieldName);
            assertEquals(handle.getColumnType(), BIGINT,
                    "Type mismatch for field: " + fieldName);
            assertEquals(handle.getOpenSearchType(), "long",
                    "OpenSearch type mismatch for field: " + fieldName);
        }
    }

    @Test
    public void testVectorColumnHandleCreation()
    {
        OpenSearchColumnHandle handle = new OpenSearchColumnHandle(
                "embedding",
                new ArrayType(REAL),
                "knn_vector",
                true,
                128);

        assertEquals(handle.getColumnName(), "embedding");
        assertEquals(handle.getColumnType(), new ArrayType(REAL));
        assertEquals(handle.getOpenSearchType(), "knn_vector");
        assertTrue(handle.isVector());
        assertEquals(handle.getVectorDimension(), 128);
    }

    @Test
    public void testNestedFieldColumnHandleCreation()
    {
        List<String> fieldPath = ImmutableList.of("reliability_scores", "answer_relevance", "score");

        OpenSearchColumnHandle handle = new OpenSearchColumnHandle(
                "reliability_scores.answer_relevance.score",
                INTEGER,
                "integer",
                false,
                0,
                true,
                "reliability_scores",
                fieldPath,
                "reliability_scores");

        assertEquals(handle.getColumnName(), "reliability_scores.answer_relevance.score");
        assertEquals(handle.getColumnType(), INTEGER);
        assertTrue(handle.isNestedField());
        assertEquals(handle.getParentFieldPath(), "reliability_scores");
        assertEquals(handle.getFieldPathList(), fieldPath);
    }

    @Test
    public void testFieldNameWithSpecialCharacters()
    {
        // Test field names with special characters
        String[] specialFieldNames = {
            "field_with_underscores",
            "field-with-dashes",
            "field.with.dots"
        };

        for (String fieldName : specialFieldNames) {
            OpenSearchColumnHandle handle = new OpenSearchColumnHandle(
                    fieldName,
                    VARCHAR,
                    "keyword");

            assertEquals(handle.getColumnName(), fieldName);
            assertEquals(handle.getColumnType(), VARCHAR);
        }
    }

    @Test
    public void testColumnMetadataGeneration()
    {
        OpenSearchColumnHandle handle = new OpenSearchColumnHandle(
                "cache_creation_input_tokens",
                BIGINT,
                "long");

        // Verify we can generate column metadata without errors
        assertEquals(handle.toColumnMetadata().getName(), "cache_creation_input_tokens");
        assertEquals(handle.toColumnMetadata().getType(), BIGINT);
    }

    @Test
    public void testEqualsAndHashCode()
    {
        OpenSearchColumnHandle handle1 = new OpenSearchColumnHandle(
                "cache_creation_input_tokens",
                BIGINT,
                "long");

        OpenSearchColumnHandle handle2 = new OpenSearchColumnHandle(
                "cache_creation_input_tokens",
                BIGINT,
                "long");

        assertEquals(handle1, handle2);
        assertEquals(handle1.hashCode(), handle2.hashCode());
    }

    @Test
    public void testToString()
    {
        OpenSearchColumnHandle handle = new OpenSearchColumnHandle(
                "cache_creation_input_tokens",
                BIGINT,
                "long");

        String toString = handle.toString();
        assertTrue(toString.contains("cache_creation_input_tokens"));
        assertTrue(toString.contains("bigint") || toString.contains("BIGINT"));
    }
}

// Made with Bob
