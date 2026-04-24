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

import com.facebook.presto.iceberg.VariantBinaryCodec.VariantBinary;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestVariantBinaryCodec
{
    @Test
    public void testNullValue()
    {
        String json = "null";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        assertNotNull(binary.getMetadata());
        assertNotNull(binary.getValue());
        assertEquals(VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue()), json);
    }

    @Test
    public void testBooleanTrue()
    {
        String json = "true";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        assertEquals(VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue()), json);
    }

    @Test
    public void testBooleanFalse()
    {
        String json = "false";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        assertEquals(VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue()), json);
    }

    @Test
    public void testSmallInteger()
    {
        String json = "42";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        assertEquals(VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue()), json);
    }

    @Test
    public void testNegativeInteger()
    {
        String json = "-100";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        assertEquals(VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue()), json);
    }

    @Test
    public void testLargeInteger()
    {
        String json = "2147483648";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        assertEquals(VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue()), json);
    }

    @Test
    public void testZero()
    {
        String json = "0";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        assertEquals(VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue()), json);
    }

    @Test
    public void testInt16Range()
    {
        // Value that requires int16 (> 127)
        String json = "1000";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        assertEquals(VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue()), json);
    }

    @Test
    public void testInt32Range()
    {
        // Value that requires int32 (> 32767)
        String json = "100000";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        assertEquals(VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue()), json);
    }

    @Test
    public void testInt64Range()
    {
        // Value that requires int64 (> 2^31 - 1)
        String json = "9999999999";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        assertEquals(VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue()), json);
    }

    @Test
    public void testDouble()
    {
        String json = "3.14";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        assertEquals(VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue()), json);
    }

    @Test
    public void testNegativeDouble()
    {
        String json = "-2.718";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        assertEquals(VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue()), json);
    }

    @Test
    public void testShortString()
    {
        String json = "\"hello\"";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        assertEquals(VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue()), json);
    }

    @Test
    public void testEmptyString()
    {
        String json = "\"\"";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        assertEquals(VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue()), json);
    }

    @Test
    public void testLongString()
    {
        // String longer than 63 bytes (exceeds short string limit)
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < 100; i++) {
            sb.append('a');
        }
        sb.append("\"");
        String json = sb.toString();
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        assertEquals(VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue()), json);
    }

    @Test
    public void testUnicodeString()
    {
        String json = "\"café ☕\"";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        assertEquals(VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue()), json);
    }

    @Test
    public void testSimpleObject()
    {
        String json = "{\"name\":\"Alice\",\"age\":30}";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        String decoded = VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue());
        // Object keys are sorted in the metadata dictionary, so the output
        // should have keys in sorted order
        assertNotNull(decoded);
        // Verify it round-trips (keys may be reordered due to sorted dictionary)
        VariantBinary binary2 = VariantBinaryCodec.fromJson(decoded);
        String decoded2 = VariantBinaryCodec.toJson(binary2.getMetadata(), binary2.getValue());
        assertEquals(decoded2, decoded);
    }

    @Test
    public void testEmptyObject()
    {
        String json = "{}";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        assertEquals(VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue()), json);
    }

    @Test
    public void testNestedObject()
    {
        String json = "{\"user\":{\"name\":\"Bob\",\"score\":95}}";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        String decoded = VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue());
        assertNotNull(decoded);
        // Verify double round-trip stability
        VariantBinary binary2 = VariantBinaryCodec.fromJson(decoded);
        assertEquals(VariantBinaryCodec.toJson(binary2.getMetadata(), binary2.getValue()), decoded);
    }

    @Test
    public void testSimpleArray()
    {
        String json = "[1,2,3]";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        assertEquals(VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue()), json);
    }

    @Test
    public void testEmptyArray()
    {
        String json = "[]";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        assertEquals(VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue()), json);
    }

    @Test
    public void testMixedArray()
    {
        String json = "[1,\"two\",true,null,3.14]";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        assertEquals(VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue()), json);
    }

    @Test
    public void testNestedArray()
    {
        String json = "[[1,2],[3,4]]";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        assertEquals(VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue()), json);
    }

    @Test
    public void testComplexDocument()
    {
        String json = "{\"name\":\"Alice\",\"scores\":[95,87,92],\"active\":true,\"address\":null}";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        String decoded = VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue());
        assertNotNull(decoded);
        // Verify double round-trip stability
        VariantBinary binary2 = VariantBinaryCodec.fromJson(decoded);
        assertEquals(VariantBinaryCodec.toJson(binary2.getMetadata(), binary2.getValue()), decoded);
    }

    @Test
    public void testDeeplyNested()
    {
        String json = "{\"a\":{\"b\":{\"c\":{\"d\":\"deep\"}}}}";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        String decoded = VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue());
        assertNotNull(decoded);
        VariantBinary binary2 = VariantBinaryCodec.fromJson(decoded);
        assertEquals(VariantBinaryCodec.toJson(binary2.getMetadata(), binary2.getValue()), decoded);
    }

    @Test
    public void testArrayOfObjects()
    {
        String json = "[{\"id\":1,\"name\":\"a\"},{\"id\":2,\"name\":\"b\"}]";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        String decoded = VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue());
        assertNotNull(decoded);
        VariantBinary binary2 = VariantBinaryCodec.fromJson(decoded);
        assertEquals(VariantBinaryCodec.toJson(binary2.getMetadata(), binary2.getValue()), decoded);
    }

    @Test
    public void testMetadataDictionary()
    {
        // Verify that the metadata dictionary is built correctly
        String json = "{\"z_key\":1,\"a_key\":2}";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        // Metadata dictionary should have keys sorted alphabetically
        String[] keys = VariantBinaryCodec.decodeMetadata(binary.getMetadata());
        assertEquals(keys.length, 2);
        assertEquals(keys[0], "a_key");
        assertEquals(keys[1], "z_key");
    }

    @Test
    public void testEmptyMetadataForPrimitives()
    {
        // Primitive values should have an empty metadata dictionary
        VariantBinary binary = VariantBinaryCodec.fromJson("42");
        String[] keys = VariantBinaryCodec.decodeMetadata(binary.getMetadata());
        assertEquals(keys.length, 0);
    }

    @Test
    public void testHeaderEncoding()
    {
        // Verify header byte construction
        assertEquals(VariantBinaryCodec.makeHeader(VariantBinaryCodec.BASIC_TYPE_PRIMITIVE, VariantBinaryCodec.PRIMITIVE_NULL), (byte) 0x00);
        assertEquals(VariantBinaryCodec.makeHeader(VariantBinaryCodec.BASIC_TYPE_PRIMITIVE, VariantBinaryCodec.PRIMITIVE_TRUE), (byte) 0x01);
        assertEquals(VariantBinaryCodec.makeHeader(VariantBinaryCodec.BASIC_TYPE_SHORT_STRING, 5), (byte) 0x45);  // 01_000101
        assertEquals(VariantBinaryCodec.makeHeader(VariantBinaryCodec.BASIC_TYPE_OBJECT, 0), (byte) 0x80);  // 10_000000
        assertEquals(VariantBinaryCodec.makeHeader(VariantBinaryCodec.BASIC_TYPE_ARRAY, 0), (byte) 0xC0);  // 11_000000
    }

    @Test
    public void testStringWithSpecialChars()
    {
        String json = "{\"key\":\"value with \\\"quotes\\\" and \\\\backslash\"}";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        String decoded = VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue());
        assertNotNull(decoded);
        VariantBinary binary2 = VariantBinaryCodec.fromJson(decoded);
        assertEquals(VariantBinaryCodec.toJson(binary2.getMetadata(), binary2.getValue()), decoded);
    }

    @Test
    public void testObjectWithMixedValues()
    {
        String json = "{\"bool\":false,\"int\":42,\"float\":1.5,\"null\":null,\"str\":\"hello\"}";
        VariantBinary binary = VariantBinaryCodec.fromJson(json);
        String decoded = VariantBinaryCodec.toJson(binary.getMetadata(), binary.getValue());
        assertNotNull(decoded);
        VariantBinary binary2 = VariantBinaryCodec.fromJson(decoded);
        assertEquals(VariantBinaryCodec.toJson(binary2.getMetadata(), binary2.getValue()), decoded);
    }

    // ---- Phase 2: isVariantBinary tests ----

    @Test
    public void testIsVariantBinaryValidObject()
    {
        VariantBinary binary = VariantBinaryCodec.fromJson("{\"a\":1}");
        assertTrue(VariantBinaryCodec.isVariantBinary(binary.getMetadata(), binary.getValue()));
    }

    @Test
    public void testIsVariantBinaryValidPrimitive()
    {
        VariantBinary binary = VariantBinaryCodec.fromJson("42");
        assertTrue(VariantBinaryCodec.isVariantBinary(binary.getMetadata(), binary.getValue()));
    }

    @Test
    public void testIsVariantBinaryValidArray()
    {
        VariantBinary binary = VariantBinaryCodec.fromJson("[1,2,3]");
        assertTrue(VariantBinaryCodec.isVariantBinary(binary.getMetadata(), binary.getValue()));
    }

    @Test
    public void testIsVariantBinaryValidString()
    {
        VariantBinary binary = VariantBinaryCodec.fromJson("\"hello\"");
        assertTrue(VariantBinaryCodec.isVariantBinary(binary.getMetadata(), binary.getValue()));
    }

    @Test
    public void testIsVariantBinaryNullMetadata()
    {
        assertFalse(VariantBinaryCodec.isVariantBinary(null, new byte[] {0}));
    }

    @Test
    public void testIsVariantBinaryNullValue()
    {
        VariantBinary binary = VariantBinaryCodec.fromJson("42");
        assertFalse(VariantBinaryCodec.isVariantBinary(binary.getMetadata(), null));
    }

    @Test
    public void testIsVariantBinaryEmptyValue()
    {
        VariantBinary binary = VariantBinaryCodec.fromJson("42");
        assertFalse(VariantBinaryCodec.isVariantBinary(binary.getMetadata(), new byte[0]));
    }

    @Test
    public void testIsVariantBinaryShortMetadata()
    {
        assertFalse(VariantBinaryCodec.isVariantBinary(new byte[] {1, 0}, new byte[] {0}));
    }

    // ---- Phase 2: getValueTypeName tests ----

    @Test
    public void testGetValueTypeNameNull()
    {
        VariantBinary binary = VariantBinaryCodec.fromJson("null");
        assertEquals(VariantBinaryCodec.getValueTypeName(binary.getValue()), "null");
    }

    @Test
    public void testGetValueTypeNameTrue()
    {
        VariantBinary binary = VariantBinaryCodec.fromJson("true");
        assertEquals(VariantBinaryCodec.getValueTypeName(binary.getValue()), "boolean");
    }

    @Test
    public void testGetValueTypeNameFalse()
    {
        VariantBinary binary = VariantBinaryCodec.fromJson("false");
        assertEquals(VariantBinaryCodec.getValueTypeName(binary.getValue()), "boolean");
    }

    @Test
    public void testGetValueTypeNameInteger()
    {
        VariantBinary binary = VariantBinaryCodec.fromJson("42");
        assertEquals(VariantBinaryCodec.getValueTypeName(binary.getValue()), "integer");
    }

    @Test
    public void testGetValueTypeNameDouble()
    {
        VariantBinary binary = VariantBinaryCodec.fromJson("3.14");
        assertEquals(VariantBinaryCodec.getValueTypeName(binary.getValue()), "double");
    }

    @Test
    public void testGetValueTypeNameShortString()
    {
        VariantBinary binary = VariantBinaryCodec.fromJson("\"hello\"");
        assertEquals(VariantBinaryCodec.getValueTypeName(binary.getValue()), "string");
    }

    @Test
    public void testGetValueTypeNameObject()
    {
        VariantBinary binary = VariantBinaryCodec.fromJson("{\"a\":1}");
        assertEquals(VariantBinaryCodec.getValueTypeName(binary.getValue()), "object");
    }

    @Test
    public void testGetValueTypeNameArray()
    {
        VariantBinary binary = VariantBinaryCodec.fromJson("[1,2]");
        assertEquals(VariantBinaryCodec.getValueTypeName(binary.getValue()), "array");
    }

    @Test
    public void testGetValueTypeNameEmptyValue()
    {
        assertEquals(VariantBinaryCodec.getValueTypeName(new byte[0]), "null");
    }

    @Test
    public void testGetValueTypeNameNullValue()
    {
        assertEquals(VariantBinaryCodec.getValueTypeName(null), "null");
    }

    // ---- Phase 2: decodeVariantAuto tests ----

    @Test
    public void testDecodeVariantAutoJsonObject()
    {
        byte[] data = "{\"a\":1}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertEquals(VariantBinaryCodec.decodeVariantAuto(data), "{\"a\":1}");
    }

    @Test
    public void testDecodeVariantAutoJsonArray()
    {
        byte[] data = "[1,2,3]".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertEquals(VariantBinaryCodec.decodeVariantAuto(data), "[1,2,3]");
    }

    @Test
    public void testDecodeVariantAutoJsonString()
    {
        byte[] data = "\"hello\"".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertEquals(VariantBinaryCodec.decodeVariantAuto(data), "\"hello\"");
    }

    @Test
    public void testDecodeVariantAutoJsonNumber()
    {
        byte[] data = "42".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertEquals(VariantBinaryCodec.decodeVariantAuto(data), "42");
    }

    @Test
    public void testDecodeVariantAutoJsonBoolean()
    {
        byte[] data = "true".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertEquals(VariantBinaryCodec.decodeVariantAuto(data), "true");
    }

    @Test
    public void testDecodeVariantAutoJsonNull()
    {
        byte[] data = "null".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertEquals(VariantBinaryCodec.decodeVariantAuto(data), "null");
    }

    @Test
    public void testDecodeVariantAutoEmpty()
    {
        assertEquals(VariantBinaryCodec.decodeVariantAuto(new byte[0]), "null");
    }

    @Test
    public void testDecodeVariantAutoNull()
    {
        assertEquals(VariantBinaryCodec.decodeVariantAuto(null), "null");
    }

    @Test
    public void testDecodeVariantAutoBinaryPrimitive()
    {
        VariantBinary binary = VariantBinaryCodec.fromJson("42");
        String decoded = VariantBinaryCodec.decodeVariantAuto(binary.getValue());
        assertEquals(decoded, "42");
    }
}
