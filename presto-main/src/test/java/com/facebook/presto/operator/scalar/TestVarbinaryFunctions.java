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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.type.VarbinaryOperators;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Base64;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.io.BaseEncoding.base16;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

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
    public void testBinaryLiteral()
            throws Exception
    {
        assertFunction("X'58F7'", VARBINARY, new SqlVarbinary(new byte[]{(byte) 0x58, (byte) 0xF7}));
    }

    @Test
    public void testLength()
            throws Exception
    {
        assertFunction("length(CAST('' AS VARBINARY))", BIGINT, 0L);
        assertFunction("length(CAST('a' AS VARBINARY))", BIGINT, 1L);
        assertFunction("length(CAST('abc' AS VARBINARY))", BIGINT, 3L);
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

    @Test
    public void testMd5()
            throws Exception
    {
        assertFunction("md5(CAST('' AS VARBINARY))", VARBINARY, sqlVarbinaryHex("D41D8CD98F00B204E9800998ECF8427E"));
        assertFunction("md5(CAST('hashme' AS VARBINARY))", VARBINARY, sqlVarbinaryHex("533F6357E0210E67D91F651BC49E1278"));
    }

    @Test
    public void testSha1()
            throws Exception
    {
        assertFunction("sha1(CAST('' AS VARBINARY))", VARBINARY, sqlVarbinaryHex("DA39A3EE5E6B4B0D3255BFEF95601890AFD80709"));
        assertFunction("sha1(CAST('hashme' AS VARBINARY))", VARBINARY, sqlVarbinaryHex("FB78992E561929A6967D5328F49413FA99048D06"));
    }

    @Test
    public void testSha256()
            throws Exception
    {
        assertFunction("sha256(CAST('' AS VARBINARY))", VARBINARY, sqlVarbinaryHex("E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"));
        assertFunction("sha256(CAST('hashme' AS VARBINARY))", VARBINARY, sqlVarbinaryHex("02208B9403A87DF9F4ED6B2EE2657EFAA589026B4CCE9ACCC8E8A5BF3D693C86"));
    }

    @Test
    public void testSha512()
            throws Exception
    {
        assertFunction("sha512(CAST('' AS VARBINARY))", VARBINARY, sqlVarbinaryHex("CF83E1357EEFB8BDF1542850D66D8007D620E4050B5715DC83F4A921D36CE9CE47D0D13C5D85F2B0FF8318D2877EEC2F63B931BD47417A81A538327AF927DA3E"));
        assertFunction("sha512(CAST('hashme' AS VARBINARY))", VARBINARY, sqlVarbinaryHex("8A4B59FB9188D09B989FF596AC9CEFBF2ED91DED8DCD9498E8BF2236814A92B23BE6867E7FC340880E514F8FDF97E1F147EA4B0FD6C2DA3557D0CF1C0B58A204"));
    }

    @Test
    public void testHashCode()
            throws Exception
    {
        Slice data = Slices.wrappedBuffer(ALL_BYTES);

        Block block = VARBINARY.createBlockBuilder(new BlockBuilderStatus(), 1, ALL_BYTES.length)
                .writeBytes(data, 0, data.length())
                .closeEntry()
                .build();

        assertEquals(VarbinaryOperators.hashCode(data), VARBINARY.hash(block, 0));
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

    private static SqlVarbinary sqlVarbinaryHex(String value)
    {
        return new SqlVarbinary(base16().decode(value));
    }
}
