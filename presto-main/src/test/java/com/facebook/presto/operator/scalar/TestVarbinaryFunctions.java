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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.type.SqlVarbinary;
import org.testng.annotations.Test;

import java.util.Base64;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.io.BaseEncoding.base16;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TestVarbinaryFunctions
        extends AbstractTestFunctions
{
    private static final byte[] ALL_BYTES;

    static {
        ALL_BYTES = new byte[256];
        for (int i = 0; i < ALL_BYTES.length; i++) {
            ALL_BYTES[i] = (byte) i;
        }
    }

    @Test
    public void testLength()
            throws Exception
    {
        assertFunction("length(CAST('' AS VARBINARY))", BIGINT, 0);
        assertFunction("length(CAST('a' AS VARBINARY))", BIGINT, 1);
        assertFunction("length(CAST('abc' AS VARBINARY))", BIGINT, 3);
    }

    @Test
    public void testToBase64()
            throws Exception
    {
        assertFunction("to_base64(CAST('' AS VARBINARY))", VARCHAR, encodeBase64(""));
        assertFunction("to_base64(CAST('a' AS VARBINARY))", VARCHAR, encodeBase64("a"));
        assertFunction("to_base64(CAST('abc' AS VARBINARY))", VARCHAR, encodeBase64("abc"));
        assertFunction("to_base64(CAST('hello world' AS VARBINARY))", VARCHAR, "aGVsbG8gd29ybGQ=");
    }

    @Test
    public void testFromBase64()
            throws Exception
    {
        assertFunction("from_base64(to_base64(CAST('' AS VARBINARY)))", VARBINARY, sqlVarbinary(""));
        assertFunction("from_base64(to_base64(CAST('a' AS VARBINARY)))", VARBINARY, sqlVarbinary("a"));
        assertFunction("from_base64(to_base64(CAST('abc' AS VARBINARY)))", VARBINARY, sqlVarbinary("abc"));
        assertFunction("from_base64(CAST(to_base64(CAST('' AS VARBINARY)) AS VARBINARY))", VARBINARY, sqlVarbinary(""));
        assertFunction("from_base64(CAST(to_base64(CAST('a' AS VARBINARY)) AS VARBINARY))", VARBINARY, sqlVarbinary("a"));
        assertFunction("from_base64(CAST(to_base64(CAST('abc' AS VARBINARY)) AS VARBINARY))", VARBINARY, sqlVarbinary("abc"));
        assertFunction(format("to_base64(from_base64('%s'))", encodeBase64(ALL_BYTES)), VARCHAR, encodeBase64(ALL_BYTES));
    }

    @Test
    public void testToBase64Url()
            throws Exception
    {
        assertFunction("to_base64url(CAST('' AS VARBINARY))", VARCHAR, encodeBase64Url(""));
        assertFunction("to_base64url(CAST('a' AS VARBINARY))", VARCHAR, encodeBase64Url("a"));
        assertFunction("to_base64url(CAST('abc' AS VARBINARY))", VARCHAR, encodeBase64Url("abc"));
        assertFunction("to_base64url(CAST('hello world' AS VARBINARY))", VARCHAR, "aGVsbG8gd29ybGQ=");
    }

    @Test
    public void testFromBase64Url()
            throws Exception
    {
        assertFunction("from_base64url(to_base64url(CAST('' AS VARBINARY)))", VARBINARY, sqlVarbinary(""));
        assertFunction("from_base64url(to_base64url(CAST('a' AS VARBINARY)))", VARBINARY, sqlVarbinary("a"));
        assertFunction("from_base64url(to_base64url(CAST('abc' AS VARBINARY)))", VARBINARY, sqlVarbinary("abc"));
        assertFunction("from_base64url(CAST(to_base64url(CAST('' AS VARBINARY)) AS VARBINARY))", VARBINARY, sqlVarbinary(""));
        assertFunction("from_base64url(CAST(to_base64url(CAST('a' AS VARBINARY)) AS VARBINARY))", VARBINARY, sqlVarbinary("a"));
        assertFunction("from_base64url(CAST(to_base64url(CAST('abc' AS VARBINARY)) AS VARBINARY))", VARBINARY, sqlVarbinary("abc"));
        assertFunction(format("to_base64url(from_base64url('%s'))", encodeBase64Url(ALL_BYTES)), VARCHAR, encodeBase64Url(ALL_BYTES));
    }

    @Test
    public void testToHex()
            throws Exception
    {
        assertFunction("to_hex(CAST('' AS VARBINARY))", VARCHAR, encodeHex(""));
        assertFunction("to_hex(CAST('a' AS VARBINARY))", VARCHAR, encodeHex("a"));
        assertFunction("to_hex(CAST('abc' AS VARBINARY))", VARCHAR, encodeHex("abc"));
        assertFunction("to_hex(CAST('hello world' AS VARBINARY))", VARCHAR, "68656C6C6F20776F726C64");
    }

    @Test
    public void testFromHex()
            throws Exception
    {
        assertFunction("from_hex('')", VARBINARY, sqlVarbinary(""));
        assertFunction("from_hex('61')", VARBINARY, sqlVarbinary("a"));
        assertFunction("from_hex('617a6f')", VARBINARY, sqlVarbinary("azo"));
        assertFunction("from_hex('617A6F')", VARBINARY, sqlVarbinary("azo"));
        assertFunction("from_hex(CAST('' AS VARBINARY))", VARBINARY, sqlVarbinary(""));
        assertFunction("from_hex(CAST('61' AS VARBINARY))", VARBINARY, sqlVarbinary("a"));
        assertFunction("from_hex(CAST('617a6F' AS VARBINARY))", VARBINARY, sqlVarbinary("azo"));
        assertFunction(format("to_hex(from_hex('%s'))", base16().encode(ALL_BYTES)), VARCHAR, base16().encode(ALL_BYTES));
        assertInvalidFunction("from_hex('f/')", INVALID_FUNCTION_ARGUMENT); // '0' - 1
        assertInvalidFunction("from_hex('f:')", INVALID_FUNCTION_ARGUMENT); // '9' + 1
        assertInvalidFunction("from_hex('f@')", INVALID_FUNCTION_ARGUMENT); // 'A' - 1
        assertInvalidFunction("from_hex('fG')", INVALID_FUNCTION_ARGUMENT); // 'F' + 1
        assertInvalidFunction("from_hex('f`')", INVALID_FUNCTION_ARGUMENT); // 'a' - 1
        assertInvalidFunction("from_hex('fg')", INVALID_FUNCTION_ARGUMENT); // 'f' + 1
        assertInvalidFunction("from_hex('fff')", INVALID_FUNCTION_ARGUMENT);
    }

    private static String encodeBase64(byte[] value)
    {
        return Base64.getEncoder().encodeToString(value);
    }

    private static String encodeBase64(String value)
    {
        return encodeBase64(value.getBytes(UTF_8));
    }

    private static String encodeBase64Url(byte[] value)
    {
        return Base64.getUrlEncoder().encodeToString(value);
    }

    private static String encodeBase64Url(String value)
    {
        return encodeBase64Url(value.getBytes(UTF_8));
    }

    private static String encodeHex(String value)
    {
        return base16().encode(value.getBytes(UTF_8));
    }

    private static SqlVarbinary sqlVarbinary(String value)
    {
        return new SqlVarbinary(value.getBytes(UTF_8));
    }
}
