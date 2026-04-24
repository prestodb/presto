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

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.iceberg.function.VariantFunctions;
import com.facebook.presto.metadata.FunctionExtractor;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;

public class TestVariantFunctions
        extends AbstractTestFunctions
{
    private static final String CATALOG_SCHEMA = "iceberg.system";

    public TestVariantFunctions()
    {
        super(TEST_SESSION, new FeaturesConfig(), new FunctionsConfig(), false);
    }

    @BeforeClass
    public void registerFunction()
    {
        ImmutableList.Builder<Class<?>> functions = ImmutableList.builder();
        functions.add(VariantFunctions.class);
        functionAssertions.addConnectorFunctions(FunctionExtractor.extractFunctions(functions.build(),
                new CatalogSchemaName("iceberg", "system")), "iceberg");
    }

    // ---- variant_get: simple field extraction ----

    @Test
    public void testVariantGetStringField()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get(JSON '{\"name\":\"Alice\",\"age\":30}', 'name')",
                JSON,
                "Alice");
    }

    @Test
    public void testVariantGetNumberField()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get(JSON '{\"name\":\"Alice\",\"age\":30}', 'age')",
                JSON,
                "30");
    }

    @Test
    public void testVariantGetBooleanField()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get(JSON '{\"active\":true}', 'active')",
                JSON,
                "true");
    }

    @Test
    public void testVariantGetNestedObject()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get(JSON '{\"address\":{\"city\":\"NYC\"}}', 'address')",
                JSON,
                "{\"city\":\"NYC\"}");
    }

    @Test
    public void testVariantGetNestedArray()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get(JSON '{\"items\":[1,2,3]}', 'items')",
                JSON,
                "[1,2,3]");
    }

    @Test
    public void testVariantGetMissingField()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get(JSON '{\"name\":\"Alice\"}', 'missing')",
                JSON,
                null);
    }

    @Test
    public void testVariantGetNonObject()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get(JSON '\"just a string\"', 'field')",
                JSON,
                null);
    }

    @Test
    public void testVariantGetNullField()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get(JSON '{\"key\":null}', 'key')",
                JSON,
                "null");
    }

    // ---- variant_get: dot-path navigation ----

    @Test
    public void testVariantGetDotPath()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get(JSON '{\"address\":{\"city\":\"NYC\"}}', 'address.city')",
                JSON,
                "NYC");
    }

    @Test
    public void testVariantGetDotPathDeep()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get(JSON '{\"a\":{\"b\":{\"c\":\"deep\"}}}', 'a.b.c')",
                JSON,
                "deep");
    }

    @Test
    public void testVariantGetDotPathMissing()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get(JSON '{\"address\":{\"city\":\"NYC\"}}', 'address.zip')",
                JSON,
                null);
    }

    @Test
    public void testVariantGetDotPathNestedObject()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get(JSON '{\"a\":{\"b\":{\"c\":1}}}', 'a.b')",
                JSON,
                "{\"c\":1}");
    }

    // ---- variant_get: array indexing ----

    @Test
    public void testVariantGetArrayIndex()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get(JSON '[10,20,30]', '[0]')",
                JSON,
                "10");
    }

    @Test
    public void testVariantGetArrayIndexLast()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get(JSON '[10,20,30]', '[2]')",
                JSON,
                "30");
    }

    @Test
    public void testVariantGetArrayOutOfBounds()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get(JSON '[10,20,30]', '[5]')",
                JSON,
                null);
    }

    @Test
    public void testVariantGetArrayOfObjects()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get(JSON '[{\"id\":1},{\"id\":2}]', '[1]')",
                JSON,
                "{\"id\":2}");
    }

    // ---- variant_get: combined dot-path + array indexing ----

    @Test
    public void testVariantGetFieldThenArray()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get(JSON '{\"items\":[1,2,3]}', 'items[1]')",
                JSON,
                "2");
    }

    @Test
    public void testVariantGetArrayThenField()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get(JSON '{\"users\":[{\"name\":\"Alice\"},{\"name\":\"Bob\"}]}', 'users[0].name')",
                JSON,
                "Alice");
    }

    @Test
    public void testVariantGetComplexPath()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get(JSON '{\"data\":{\"rows\":[{\"v\":99}]}}', 'data.rows[0].v')",
                JSON,
                "99");
    }

    // ---- variant_keys ----

    @Test
    public void testVariantKeysSimple()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_keys(JSON '{\"name\":\"Alice\",\"age\":30}')",
                JSON,
                "[\"name\",\"age\"]");
    }

    @Test
    public void testVariantKeysEmpty()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_keys(JSON '{}')",
                JSON,
                "[]");
    }

    @Test
    public void testVariantKeysNonObject()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_keys(JSON '[1,2,3]')",
                JSON,
                null);
    }

    @Test
    public void testVariantKeysScalar()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_keys(JSON '42')",
                JSON,
                null);
    }

    @Test
    public void testVariantKeysNested()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_keys(JSON '{\"a\":{\"b\":1},\"c\":[1]}')",
                JSON,
                "[\"a\",\"c\"]");
    }

    // ---- variant_type ----

    @DataProvider(name = "variantTypeCases")
    public Object[][] variantTypeCases()
    {
        return new Object[][] {
                {"{\"a\":1}", "object"},
                {"[1,2]", "array"},
                {"\"hello\"", "string"},
                {"42", "number"},
                {"3.14", "number"},
                {"true", "boolean"},
                {"null", "null"},
        };
    }

    @Test(dataProvider = "variantTypeCases")
    public void testVariantType(String json, String expectedType)
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_type(JSON '" + json + "')",
                VARCHAR,
                expectedType);
    }

    // ---- to_variant (Phase 5: CAST) ----

    @Test
    public void testToVariantObject()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".to_variant(JSON '{\"name\":\"Alice\"}')",
                JSON,
                "{\"name\":\"Alice\"}");
    }

    @Test
    public void testToVariantArray()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".to_variant(JSON '[1,2,3]')",
                JSON,
                "[1,2,3]");
    }

    @Test
    public void testToVariantScalar()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".to_variant(JSON '42')",
                JSON,
                "42");
    }

    @Test
    public void testToVariantBoolean()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".to_variant(JSON 'true')",
                JSON,
                "true");
    }

    @Test
    public void testToVariantNull()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".to_variant(JSON 'null')",
                JSON,
                "null");
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testToVariantInvalid()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".to_variant(JSON 'not valid json')",
                JSON,
                null);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testToVariantTrailingContent()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".to_variant(JSON '{\"a\":1} extra')",
                JSON,
                null);
    }

    // ---- parse_variant (binary codec round-trip) ----

    @Test
    public void testParseVariantSimpleObject()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".parse_variant('{\"a\":1}')",
                JSON,
                "{\"a\":1}");
    }

    @Test
    public void testParseVariantArray()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".parse_variant('[1,2,3]')",
                JSON,
                "[1,2,3]");
    }

    @Test
    public void testParseVariantString()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".parse_variant('\"hello\"')",
                JSON,
                "\"hello\"");
    }

    @Test
    public void testParseVariantNumber()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".parse_variant('42')",
                JSON,
                "42");
    }

    @Test
    public void testParseVariantBoolean()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".parse_variant('true')",
                JSON,
                "true");
    }

    @Test
    public void testParseVariantNull()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".parse_variant('null')",
                JSON,
                "null");
    }

    @Test
    public void testParseVariantNestedObject()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".parse_variant('{\"a\":{\"b\":1},\"c\":[true,false]}')",
                JSON,
                "{\"a\":{\"b\":1},\"c\":[true,false]}");
    }

    // ---- variant_to_json ----

    @Test
    public void testVariantToJsonObject()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_to_json(JSON '{\"name\":\"Alice\"}')",
                VARCHAR,
                "{\"name\":\"Alice\"}");
    }

    @Test
    public void testVariantToJsonArray()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_to_json(JSON '[1,2,3]')",
                VARCHAR,
                "[1,2,3]");
    }

    @Test
    public void testVariantToJsonScalar()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_to_json(JSON '42')",
                VARCHAR,
                "42");
    }

    // ---- variant_binary_roundtrip ----

    @DataProvider(name = "variantBinaryRoundtripCases")
    public Object[][] variantBinaryRoundtripCases()
    {
        return new Object[][] {
                {"{\"a\":1,\"b\":\"hello\"}"},
                {"[1,true,\"text\",null]"},
                {"{\"outer\":{\"inner\":[1,2]}}"},
                {"42"},
                {"\"hello world\""},
                {"true"},
                {"null"},
        };
    }

    @Test(dataProvider = "variantBinaryRoundtripCases")
    public void testVariantBinaryRoundtrip(String json)
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_binary_roundtrip('" + json + "')",
                JSON,
                json);
    }
}
