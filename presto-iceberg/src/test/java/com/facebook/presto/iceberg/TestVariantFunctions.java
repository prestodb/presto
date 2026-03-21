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
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
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
                CATALOG_SCHEMA + ".variant_get('{\"name\":\"Alice\",\"age\":30}', 'name')",
                VARCHAR,
                "Alice");
    }

    @Test
    public void testVariantGetNumberField()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get('{\"name\":\"Alice\",\"age\":30}', 'age')",
                VARCHAR,
                "30");
    }

    @Test
    public void testVariantGetBooleanField()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get('{\"active\":true}', 'active')",
                VARCHAR,
                "true");
    }

    @Test
    public void testVariantGetNestedObject()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get('{\"address\":{\"city\":\"NYC\"}}', 'address')",
                VARCHAR,
                "{\"city\":\"NYC\"}");
    }

    @Test
    public void testVariantGetNestedArray()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get('{\"items\":[1,2,3]}', 'items')",
                VARCHAR,
                "[1,2,3]");
    }

    @Test
    public void testVariantGetMissingField()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get('{\"name\":\"Alice\"}', 'missing')",
                VARCHAR,
                null);
    }

    @Test
    public void testVariantGetNonObject()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get('\"just a string\"', 'field')",
                VARCHAR,
                null);
    }

    @Test
    public void testVariantGetNullField()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get('{\"key\":null}', 'key')",
                VARCHAR,
                "null");
    }

    // ---- variant_get: dot-path navigation ----

    @Test
    public void testVariantGetDotPath()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get('{\"address\":{\"city\":\"NYC\"}}', 'address.city')",
                VARCHAR,
                "NYC");
    }

    @Test
    public void testVariantGetDotPathDeep()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get('{\"a\":{\"b\":{\"c\":\"deep\"}}}', 'a.b.c')",
                VARCHAR,
                "deep");
    }

    @Test
    public void testVariantGetDotPathMissing()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get('{\"address\":{\"city\":\"NYC\"}}', 'address.zip')",
                VARCHAR,
                null);
    }

    @Test
    public void testVariantGetDotPathNestedObject()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get('{\"a\":{\"b\":{\"c\":1}}}', 'a.b')",
                VARCHAR,
                "{\"c\":1}");
    }

    // ---- variant_get: array indexing ----

    @Test
    public void testVariantGetArrayIndex()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get('[10,20,30]', '[0]')",
                VARCHAR,
                "10");
    }

    @Test
    public void testVariantGetArrayIndexLast()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get('[10,20,30]', '[2]')",
                VARCHAR,
                "30");
    }

    @Test
    public void testVariantGetArrayOutOfBounds()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get('[10,20,30]', '[5]')",
                VARCHAR,
                null);
    }

    @Test
    public void testVariantGetArrayOfObjects()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get('[{\"id\":1},{\"id\":2}]', '[1]')",
                VARCHAR,
                "{\"id\":2}");
    }

    // ---- variant_get: combined dot-path + array indexing ----

    @Test
    public void testVariantGetFieldThenArray()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get('{\"items\":[1,2,3]}', 'items[1]')",
                VARCHAR,
                "2");
    }

    @Test
    public void testVariantGetArrayThenField()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get('{\"users\":[{\"name\":\"Alice\"},{\"name\":\"Bob\"}]}', 'users[0].name')",
                VARCHAR,
                "Alice");
    }

    @Test
    public void testVariantGetComplexPath()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_get('{\"data\":{\"rows\":[{\"v\":99}]}}', 'data.rows[0].v')",
                VARCHAR,
                "99");
    }

    // ---- variant_keys ----

    @Test
    public void testVariantKeysSimple()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_keys('{\"name\":\"Alice\",\"age\":30}')",
                VARCHAR,
                "[\"name\",\"age\"]");
    }

    @Test
    public void testVariantKeysEmpty()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_keys('{}')",
                VARCHAR,
                "[]");
    }

    @Test
    public void testVariantKeysNonObject()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_keys('[1,2,3]')",
                VARCHAR,
                null);
    }

    @Test
    public void testVariantKeysScalar()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_keys('42')",
                VARCHAR,
                null);
    }

    @Test
    public void testVariantKeysNested()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_keys('{\"a\":{\"b\":1},\"c\":[1]}')",
                VARCHAR,
                "[\"a\",\"c\"]");
    }

    // ---- variant_type ----

    @Test
    public void testVariantTypeObject()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_type('{\"a\":1}')",
                VARCHAR,
                "object");
    }

    @Test
    public void testVariantTypeArray()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_type('[1,2]')",
                VARCHAR,
                "array");
    }

    @Test
    public void testVariantTypeString()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_type('\"hello\"')",
                VARCHAR,
                "string");
    }

    @Test
    public void testVariantTypeNumber()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_type('42')",
                VARCHAR,
                "number");
    }

    @Test
    public void testVariantTypeFloat()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_type('3.14')",
                VARCHAR,
                "number");
    }

    @Test
    public void testVariantTypeBoolean()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_type('true')",
                VARCHAR,
                "boolean");
    }

    @Test
    public void testVariantTypeNull()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_type('null')",
                VARCHAR,
                "null");
    }

    // ---- to_variant (Phase 5: CAST) ----

    @Test
    public void testToVariantObject()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".to_variant('{\"name\":\"Alice\"}')",
                VARCHAR,
                "{\"name\":\"Alice\"}");
    }

    @Test
    public void testToVariantArray()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".to_variant('[1,2,3]')",
                VARCHAR,
                "[1,2,3]");
    }

    @Test
    public void testToVariantScalar()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".to_variant('42')",
                VARCHAR,
                "42");
    }

    @Test
    public void testToVariantBoolean()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".to_variant('true')",
                VARCHAR,
                "true");
    }

    @Test
    public void testToVariantNull()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".to_variant('null')",
                VARCHAR,
                "null");
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testToVariantInvalid()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".to_variant('not valid json')",
                VARCHAR,
                null);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testToVariantTrailingContent()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".to_variant('{\"a\":1} extra')",
                VARCHAR,
                null);
    }

    // ---- parse_variant (binary codec round-trip) ----

    @Test
    public void testParseVariantSimpleObject()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".parse_variant('{\"a\":1}')",
                VARCHAR,
                "{\"a\":1}");
    }

    @Test
    public void testParseVariantArray()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".parse_variant('[1,2,3]')",
                VARCHAR,
                "[1,2,3]");
    }

    @Test
    public void testParseVariantString()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".parse_variant('\"hello\"')",
                VARCHAR,
                "\"hello\"");
    }

    @Test
    public void testParseVariantNumber()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".parse_variant('42')",
                VARCHAR,
                "42");
    }

    @Test
    public void testParseVariantBoolean()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".parse_variant('true')",
                VARCHAR,
                "true");
    }

    @Test
    public void testParseVariantNull()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".parse_variant('null')",
                VARCHAR,
                "null");
    }

    @Test
    public void testParseVariantNestedObject()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".parse_variant('{\"a\":{\"b\":1},\"c\":[true,false]}')",
                VARCHAR,
                "{\"a\":{\"b\":1},\"c\":[true,false]}");
    }

    // ---- variant_to_json ----

    @Test
    public void testVariantToJsonObject()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_to_json('{\"name\":\"Alice\"}')",
                VARCHAR,
                "{\"name\":\"Alice\"}");
    }

    @Test
    public void testVariantToJsonArray()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_to_json('[1,2,3]')",
                VARCHAR,
                "[1,2,3]");
    }

    @Test
    public void testVariantToJsonScalar()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_to_json('42')",
                VARCHAR,
                "42");
    }

    // ---- variant_binary_roundtrip ----

    @Test
    public void testVariantBinaryRoundtripObject()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_binary_roundtrip('{\"a\":1,\"b\":\"hello\"}')",
                VARCHAR,
                "{\"a\":1,\"b\":\"hello\"}");
    }

    @Test
    public void testVariantBinaryRoundtripArray()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_binary_roundtrip('[1,true,\"text\",null]')",
                VARCHAR,
                "[1,true,\"text\",null]");
    }

    @Test
    public void testVariantBinaryRoundtripNested()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_binary_roundtrip('{\"outer\":{\"inner\":[1,2]}}')",
                VARCHAR,
                "{\"outer\":{\"inner\":[1,2]}}");
    }

    @Test
    public void testVariantBinaryRoundtripScalar()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_binary_roundtrip('42')",
                VARCHAR,
                "42");
    }

    @Test
    public void testVariantBinaryRoundtripString()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_binary_roundtrip('\"hello world\"')",
                VARCHAR,
                "\"hello world\"");
    }

    @Test
    public void testVariantBinaryRoundtripBoolean()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_binary_roundtrip('true')",
                VARCHAR,
                "true");
    }

    @Test
    public void testVariantBinaryRoundtripNull()
    {
        functionAssertions.assertFunction(
                CATALOG_SCHEMA + ".variant_binary_roundtrip('null')",
                VARCHAR,
                "null");
    }
}
