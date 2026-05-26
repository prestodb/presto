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
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.VarcharType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * Tests for NestedValueExtractor class.
 */
@Test(singleThreaded = true)
public class TestNestedValueExtractor
{
    private NestedValueExtractor extractor;

    @BeforeMethod
    public void setUp()
    {
        extractor = new NestedValueExtractor();
    }

    @Test
    public void testExtractSimpleNestedLong()
    {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> tokenUsage = new HashMap<>();
        tokenUsage.put("total_tokens", 90616L);
        document.put("token_usage", tokenUsage);

        Object result = extractor.extractNestedValue(
                document,
                Arrays.asList("token_usage", "total_tokens"),
                BigintType.BIGINT);

        assertEquals(result, 90616L);
    }

    @Test
    public void testExtractDeeplyNestedValue()
    {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> level1 = new HashMap<>();
        Map<String, Object> level2 = new HashMap<>();
        Map<String, Object> level3 = new HashMap<>();
        level3.put("value", 42L);
        level2.put("c", level3);
        level1.put("b", level2);
        document.put("a", level1);

        Object result = extractor.extractNestedValue(
                document,
                Arrays.asList("a", "b", "c", "value"),
                BigintType.BIGINT);

        assertEquals(result, 42L);
    }

    @Test
    public void testExtractMissingField()
    {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> tokenUsage = new HashMap<>();
        tokenUsage.put("total_tokens", 90616L);
        document.put("token_usage", tokenUsage);

        Object result = extractor.extractNestedValue(
                document,
                Arrays.asList("token_usage", "missing_field"),
                BigintType.BIGINT);

        assertNull(result);
    }

    @Test
    public void testExtractMissingParent()
    {
        Map<String, Object> document = new HashMap<>();
        document.put("other_field", "value");

        Object result = extractor.extractNestedValue(
                document,
                Arrays.asList("token_usage", "total_tokens"),
                BigintType.BIGINT);

        assertNull(result);
    }

    @Test
    public void testExtractWithNullDocument()
    {
        Object result = extractor.extractNestedValue(
                null,
                Arrays.asList("token_usage", "total_tokens"),
                BigintType.BIGINT);

        assertNull(result);
    }

    @Test
    public void testExtractWithEmptyPath()
    {
        Map<String, Object> document = new HashMap<>();
        document.put("field", "value");

        Object result = extractor.extractNestedValue(
                document,
                Collections.emptyList(),
                VarcharType.VARCHAR);

        assertNull(result);
    }

    @Test
    public void testExtractWithNullPath()
    {
        Map<String, Object> document = new HashMap<>();
        document.put("field", "value");

        Object result = extractor.extractNestedValue(
                document,
                null,
                VarcharType.VARCHAR);

        assertNull(result);
    }

    @Test
    public void testTypeConversionIntegerToLong()
    {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> nested = new HashMap<>();
        nested.put("count", 100);  // Integer
        document.put("data", nested);

        Object result = extractor.extractNestedValue(
                document,
                Arrays.asList("data", "count"),
                BigintType.BIGINT);

        assertEquals(result, 100L);
    }

    @Test
    public void testTypeConversionFloatToDouble()
    {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> nested = new HashMap<>();
        nested.put("price", 19.99f);  // Float
        document.put("product", nested);

        Object result = extractor.extractNestedValue(
                document,
                Arrays.asList("product", "price"),
                DoubleType.DOUBLE);

        assertEquals(((Double) result), 19.99, 0.01);
    }

    @Test
    public void testExtractBoolean()
    {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> nested = new HashMap<>();
        nested.put("active", true);
        document.put("status", nested);

        Object result = extractor.extractNestedValue(
                document,
                Arrays.asList("status", "active"),
                BooleanType.BOOLEAN);

        assertEquals(result, true);
    }

    @Test
    public void testExtractString()
    {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> nested = new HashMap<>();
        nested.put("name", "John Doe");
        document.put("user", nested);

        Object result = extractor.extractNestedValue(
                document,
                Arrays.asList("user", "name"),
                VarcharType.VARCHAR);

        assertEquals(result, "John Doe");
    }

    @Test
    public void testExtractMultipleFieldsFromSameParent()
    {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> tokenUsage = new HashMap<>();
        tokenUsage.put("total_tokens", 90616L);
        tokenUsage.put("prompt_tokens", 45308L);
        tokenUsage.put("completion_tokens", 45308L);
        document.put("token_usage", tokenUsage);

        Object total = extractor.extractNestedValue(
                document,
                Arrays.asList("token_usage", "total_tokens"),
                BigintType.BIGINT);
        Object prompt = extractor.extractNestedValue(
                document,
                Arrays.asList("token_usage", "prompt_tokens"),
                BigintType.BIGINT);
        Object completion = extractor.extractNestedValue(
                document,
                Arrays.asList("token_usage", "completion_tokens"),
                BigintType.BIGINT);

        assertEquals(total, 90616L);
        assertEquals(prompt, 45308L);
        assertEquals(completion, 45308L);
    }

    @Test
    public void testExtractRootLevelField()
    {
        Map<String, Object> document = new HashMap<>();
        document.put("id", "12345");

        Object result = extractor.extractNestedValue(
                document,
                Collections.singletonList("id"),
                VarcharType.VARCHAR);

        assertEquals(result, "12345");
    }

    @Test
    public void testExtractNullValue()
    {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> nested = new HashMap<>();
        nested.put("nullable_field", null);
        document.put("data", nested);

        Object result = extractor.extractNestedValue(
                document,
                Arrays.asList("data", "nullable_field"),
                VarcharType.VARCHAR);

        assertNull(result);
    }

    @Test
    public void testExtractFromComplexDocument()
    {
        Map<String, Object> document = new HashMap<>();

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("id", "doc123");
        metadata.put("timestamp", 1234567890L);

        Map<String, Object> tokenUsage = new HashMap<>();
        tokenUsage.put("total_tokens", 90616L);
        tokenUsage.put("prompt_tokens", 45308L);

        Map<String, Object> model = new HashMap<>();
        model.put("name", "gpt-4");
        model.put("version", "0613");

        document.put("metadata", metadata);
        document.put("token_usage", tokenUsage);
        document.put("model", model);

        assertEquals(
                extractor.extractNestedValue(document, Arrays.asList("metadata", "id"), VarcharType.VARCHAR),
                "doc123");
        assertEquals(
                extractor.extractNestedValue(document, Arrays.asList("token_usage", "total_tokens"), BigintType.BIGINT),
                90616L);
        assertEquals(
                extractor.extractNestedValue(document, Arrays.asList("model", "name"), VarcharType.VARCHAR),
                "gpt-4");
    }

    @Test
    public void testExtractFlatValue()
    {
        Map<String, Object> document = new HashMap<>();
        document.put("count", 42L);
        document.put("name", "test");
        document.put("active", true);

        assertEquals(extractor.extractFlatValue(document, "count", BigintType.BIGINT), 42L);
        assertEquals(extractor.extractFlatValue(document, "name", VarcharType.VARCHAR), "test");
        assertEquals(extractor.extractFlatValue(document, "active", BooleanType.BOOLEAN), true);
    }

    @Test
    public void testExtractFlatValueMissing()
    {
        Map<String, Object> document = new HashMap<>();
        document.put("field", "value");

        assertNull(extractor.extractFlatValue(document, "missing", VarcharType.VARCHAR));
    }

    @Test
    public void testExtractFlatValueNullDocument()
    {
        assertNull(extractor.extractFlatValue(null, "field", VarcharType.VARCHAR));
    }

    @Test
    public void testExtractFlatValueNullFieldName()
    {
        Map<String, Object> document = new HashMap<>();
        document.put("field", "value");

        assertNull(extractor.extractFlatValue(document, null, VarcharType.VARCHAR));
    }

    @Test
    public void testTypeConversionNumberToString()
    {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> nested = new HashMap<>();
        nested.put("count", 42);
        document.put("data", nested);

        Object result = extractor.extractNestedValue(
                document,
                Arrays.asList("data", "count"),
                VarcharType.VARCHAR);

        assertEquals(result, "42");
    }

    @Test
    public void testExtractZeroValue()
    {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> nested = new HashMap<>();
        nested.put("count", 0L);
        document.put("data", nested);

        Object result = extractor.extractNestedValue(
                document,
                Arrays.asList("data", "count"),
                BigintType.BIGINT);

        assertEquals(result, 0L);
    }

    @Test
    public void testExtractNegativeValue()
    {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> nested = new HashMap<>();
        nested.put("balance", -100L);
        document.put("account", nested);

        Object result = extractor.extractNestedValue(
                document,
                Arrays.asList("account", "balance"),
                BigintType.BIGINT);

        assertEquals(result, -100L);
    }

    @Test
    public void testExtractLargeNumber()
    {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> nested = new HashMap<>();
        nested.put("big_value", Long.MAX_VALUE);
        document.put("data", nested);

        Object result = extractor.extractNestedValue(
                document,
                Arrays.asList("data", "big_value"),
                BigintType.BIGINT);

        assertEquals(result, Long.MAX_VALUE);
    }

    @Test
    public void testExtractEmptyString()
    {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> nested = new HashMap<>();
        nested.put("text", "");
        document.put("data", nested);

        Object result = extractor.extractNestedValue(
                document,
                Arrays.asList("data", "text"),
                VarcharType.VARCHAR);

        assertEquals(result, "");
    }

    @Test
    public void testExtractWhitespaceString()
    {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> nested = new HashMap<>();
        nested.put("text", "   ");
        document.put("data", nested);

        Object result = extractor.extractNestedValue(
                document,
                Arrays.asList("data", "text"),
                VarcharType.VARCHAR);

        assertEquals(result, "   ");
    }

    @Test
    public void testExtractIntegerType()
    {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> nested = new HashMap<>();
        nested.put("count", 100);
        document.put("data", nested);

        Object result = extractor.extractNestedValue(
                document,
                Arrays.asList("data", "count"),
                IntegerType.INTEGER);

        assertEquals(result, 100);
    }

    @Test
    public void testExtractNestedArrayFieldAsList()
    {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> tokenUsage = new HashMap<>();
        tokenUsage.put("models_used", Arrays.asList("anthropic/claude-haiku-4-5-20251001"));
        tokenUsage.put("total_tokens", 336777);
        document.put("token_usage", tokenUsage);

        Object result = extractor.extractNestedValue(
                document,
                Arrays.asList("token_usage", "models_used"),
                new ArrayType(VarcharType.VARCHAR));

        assertEquals(result instanceof List, true);
        List<?> resultList = (List<?>) result;
        assertEquals(resultList.size(), 1);
        assertEquals(resultList.get(0), "anthropic/claude-haiku-4-5-20251001");
    }

    @Test
    public void testExtractNestedArrayFieldAsObjectArray()
    {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> tokenUsage = new HashMap<>();
        tokenUsage.put("models_used", new Object[]{"anthropic/claude-haiku-4-5-20251001", "gpt-4"});
        tokenUsage.put("total_tokens", 336777);
        document.put("token_usage", tokenUsage);

        Object result = extractor.extractNestedValue(
                document,
                Arrays.asList("token_usage", "models_used"),
                new ArrayType(VarcharType.VARCHAR));

        assertEquals(result.getClass(), java.util.ArrayList.class);
        java.util.List<?> resultList = (java.util.List<?>) result;
        assertEquals(resultList.size(), 2);
        assertEquals(resultList.get(0), "anthropic/claude-haiku-4-5-20251001");
        assertEquals(resultList.get(1), "gpt-4");
    }

    @Test
    public void testExtractNestedComplexObjectAsVarchar()
    {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> tokenUsage = new HashMap<>();
        tokenUsage.put("total_tokens", 336777);
        tokenUsage.put("models_used", Arrays.asList("model1", "model2"));
        document.put("token_usage", tokenUsage);

        Object result = extractor.extractNestedValue(
                document,
                Arrays.asList("token_usage"),
                VarcharType.VARCHAR);

        assertEquals(result.getClass(), String.class);
        String jsonString = (String) result;
        assertEquals(jsonString.contains("total_tokens"), true);
        assertEquals(jsonString.contains("models_used"), true);
    }
}
