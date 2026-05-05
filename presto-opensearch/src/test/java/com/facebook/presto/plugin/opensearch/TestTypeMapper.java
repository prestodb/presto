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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.plugin.opensearch.types.TypeMapper;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests for TypeMapper to ensure robust handling of various type strings,
 * including malformed and edge cases.
 */
public class TestTypeMapper
{
    @Test
    public void testStandardOpenSearchTypes()
    {
        // Text types
        assertEquals(TypeMapper.toPrestoType("text"), VARCHAR);
        assertEquals(TypeMapper.toPrestoType("keyword"), VARCHAR);
        assertEquals(TypeMapper.toPrestoType("ip"), VARCHAR);

        // Numeric types
        assertEquals(TypeMapper.toPrestoType("long"), BIGINT);
        assertEquals(TypeMapper.toPrestoType("integer"), INTEGER);
        assertEquals(TypeMapper.toPrestoType("short"), SMALLINT);
        assertEquals(TypeMapper.toPrestoType("byte"), TINYINT);
        assertEquals(TypeMapper.toPrestoType("double"), DOUBLE);
        assertEquals(TypeMapper.toPrestoType("scaled_float"), DOUBLE);
        assertEquals(TypeMapper.toPrestoType("float"), REAL);
        assertEquals(TypeMapper.toPrestoType("half_float"), REAL);

        // Other types
        assertEquals(TypeMapper.toPrestoType("boolean"), BOOLEAN);
        assertEquals(TypeMapper.toPrestoType("date"), DATE);
        assertEquals(TypeMapper.toPrestoType("binary"), VARBINARY);
        assertEquals(TypeMapper.toPrestoType("nested"), VARCHAR);
        assertEquals(TypeMapper.toPrestoType("object"), VARCHAR);
    }

    @Test
    public void testVectorTypes()
    {
        // knn_vector should map to ARRAY(REAL)
        Type knnVectorType = TypeMapper.toPrestoType("knn_vector");
        assertTrue(knnVectorType instanceof ArrayType);
        assertEquals(((ArrayType) knnVectorType).getElementType(), REAL);

        // Vector field names with float type should map to ARRAY(REAL)
        Type embeddingType = TypeMapper.toPrestoType("float", Map.of(), "embedding");
        assertTrue(embeddingType instanceof ArrayType);
        assertEquals(((ArrayType) embeddingType).getElementType(), REAL);

        Type vectorFieldType = TypeMapper.toPrestoType("float", Map.of(), "knn_vector_field");
        assertTrue(vectorFieldType instanceof ArrayType);
        assertEquals(((ArrayType) vectorFieldType).getElementType(), REAL);
    }

    @Test
    public void testCaseInsensitivity()
    {
        assertEquals(TypeMapper.toPrestoType("TEXT"), VARCHAR);
        assertEquals(TypeMapper.toPrestoType("Text"), VARCHAR);
        assertEquals(TypeMapper.toPrestoType("BIGINT"), BIGINT); // Now accepts Presto type names for malformed string handling
        assertEquals(TypeMapper.toPrestoType("LONG"), BIGINT);
        assertEquals(TypeMapper.toPrestoType("Long"), BIGINT);
    }

    @Test
    public void testUnknownTypes()
    {
        // Unknown types should default to VARCHAR
        assertEquals(TypeMapper.toPrestoType("unknown_type"), VARCHAR);
        assertEquals(TypeMapper.toPrestoType("custom_plugin_type"), VARCHAR);
        assertEquals(TypeMapper.toPrestoType("geo_point"), VARCHAR);
    }

    @Test
    public void testMalformedTypeStrings()
    {
        // Test the specific case from the bug: "cache_creation_input_tokens bigint"
        // Now we accept Presto type names (like "bigint") to handle malformed serialization strings
        // The sanitizer extracts "bigint" from the second position and maps it correctly to BIGINT
        assertEquals(TypeMapper.toPrestoType("cache_creation_input_tokens bigint"), BIGINT);

        // Test with known OpenSearch type in second position - should extract and use it
        assertEquals(TypeMapper.toPrestoType("field_name long"), BIGINT);
        assertEquals(TypeMapper.toPrestoType("some_field integer"), INTEGER);
        assertEquals(TypeMapper.toPrestoType("my_field text"), VARCHAR);

        // Test with multiple spaces
        assertEquals(TypeMapper.toPrestoType("field   name   long"), BIGINT);

        // Test with known type in first position (should use first token)
        assertEquals(TypeMapper.toPrestoType("long field_name"), BIGINT);
    }

    @Test
    public void testComplexFieldNames()
    {
        // Field names that might be confused with types
        assertEquals(TypeMapper.toPrestoType("long", Map.of(), "cache_creation_input_tokens"), BIGINT);
        assertEquals(TypeMapper.toPrestoType("integer", Map.of(), "bigint_value"), INTEGER);
        assertEquals(TypeMapper.toPrestoType("text", Map.of(), "array_of_strings"), VARCHAR);
        assertEquals(TypeMapper.toPrestoType("keyword", Map.of(), "varchar_field"), VARCHAR);

        // Underscore-heavy field names
        assertEquals(TypeMapper.toPrestoType("long", Map.of(), "user_session_cache_tokens"), BIGINT);
        assertEquals(TypeMapper.toPrestoType("text", Map.of(), "llm_response_text_content"), VARCHAR);
    }

    @Test
    public void testNullAndEmptyTypes()
    {
        // Null type should default to VARCHAR
        assertEquals(TypeMapper.toPrestoType(null), VARCHAR);
        assertEquals(TypeMapper.toPrestoType(null, Map.of()), VARCHAR);
        assertEquals(TypeMapper.toPrestoType(null, Map.of(), "field_name"), VARCHAR);

        // Empty type should default to VARCHAR
        assertEquals(TypeMapper.toPrestoType(""), VARCHAR);
        assertEquals(TypeMapper.toPrestoType("   "), VARCHAR);
        assertEquals(TypeMapper.toPrestoType("", Map.of(), "field_name"), VARCHAR);
    }

    @Test
    public void testWhitespaceHandling()
    {
        // Leading/trailing whitespace should be trimmed
        assertEquals(TypeMapper.toPrestoType("  long  "), BIGINT);
        assertEquals(TypeMapper.toPrestoType("\tinteger\t"), INTEGER);
        assertEquals(TypeMapper.toPrestoType("\ntext\n"), VARCHAR);

        // Whitespace with field name
        assertEquals(TypeMapper.toPrestoType("  field_name   long  "), BIGINT);
    }

    @Test
    public void testRealWorldProblematicFieldNames()
    {
        // Real-world field names from LLM/analytics systems that might cause issues
        String[] problematicFieldNames = {
            "cache_creation_input_tokens",
            "cache_creation_output_tokens",
            "total_input_tokens",
            "prompt_tokens_used",
            "completion_tokens_generated",
            "embedding_vector_dimension",
            "model_response_time_ms",
            "api_call_timestamp",
            "user_query_text",
            "llm_model_version"
        };

        // All should work correctly with their respective types
        for (String fieldName : problematicFieldNames) {
            assertEquals(TypeMapper.toPrestoType("long", Map.of(), fieldName), BIGINT);
            assertEquals(TypeMapper.toPrestoType("text", Map.of(), fieldName), VARCHAR);
            assertEquals(TypeMapper.toPrestoType("keyword", Map.of(), fieldName), VARCHAR);
        }
    }

    @Test
    public void testTypeStringWithSpecialCharacters()
    {
        // Type strings with special characters should be handled gracefully
        assertEquals(TypeMapper.toPrestoType("long-type"), VARCHAR); // Unknown type
        assertEquals(TypeMapper.toPrestoType("type.long"), VARCHAR); // Unknown type
        assertEquals(TypeMapper.toPrestoType("long_type"), VARCHAR); // Unknown type
    }

    @Test
    public void testPropertiesParameter()
    {
        // Test that properties parameter doesn't affect basic type mapping
        Map<String, Object> properties = Map.of(
                "index", "true",
                "store", "true");

        assertEquals(TypeMapper.toPrestoType("long", properties), BIGINT);
        assertEquals(TypeMapper.toPrestoType("text", properties), VARCHAR);
        assertEquals(TypeMapper.toPrestoType("boolean", properties), BOOLEAN);
    }
}

// Made with Bob
