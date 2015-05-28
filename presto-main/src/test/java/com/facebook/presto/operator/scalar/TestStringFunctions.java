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
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestStringFunctions
        extends AbstractTestFunctions
{
    private TestStringFunctions()
    {
        registerScalar(getClass());
    }

    @ScalarFunction(value = "utf8", deterministic = false)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice convertBinaryToVarchar(@SqlType(StandardTypes.VARBINARY) Slice binary)
    {
        return binary;
    }

    @Test
    public void testChr()
    {
        assertFunction("CHR(65)", VARCHAR, "A");
        assertFunction("CHR(9731)", VARCHAR, "\u2603");
        assertFunction("CHR(131210)", VARCHAR, new String(Character.toChars(131210)));
        assertFunction("CHR(0)", VARCHAR, "\0");

        assertInvalidFunction("CHR(-1)", "Not a valid Unicode code point: -1");
        assertInvalidFunction("CHR(1234567)", "Not a valid Unicode code point: 1234567");
        assertInvalidFunction("CHR(8589934592)", "Not a valid Unicode code point: 8589934592");
    }

    @Test
    public void testConcat()
    {
        assertFunction("CONCAT('hello', ' world')", VARCHAR, "hello world");
        assertFunction("CONCAT('', '')", VARCHAR, "");
        assertFunction("CONCAT('what', '')", VARCHAR, "what");
        assertFunction("CONCAT('', 'what')", VARCHAR, "what");
        assertFunction("CONCAT(CONCAT('this', ' is'), ' cool')", VARCHAR, "this is cool");
        assertFunction("CONCAT('this', CONCAT(' is', ' cool'))", VARCHAR, "this is cool");
        //
        // Test concat for non-ASCII
        assertFunction("CONCAT('hello na\u00EFve', ' world')", VARCHAR, "hello na\u00EFve world");
        assertFunction("CONCAT('\uD801\uDC2D', 'end')", VARCHAR, "\uD801\uDC2Dend");
        assertFunction("CONCAT(CONCAT('\u4FE1\u5FF5', ',\u7231'), ',\u5E0C\u671B')", VARCHAR, "\u4FE1\u5FF5,\u7231,\u5E0C\u671B");
    }

    @Test
    public void testLength()
    {
        assertFunction("LENGTH('')", BIGINT, 0);
        assertFunction("LENGTH('hello')", BIGINT, 5);
        assertFunction("LENGTH('Quadratically')", BIGINT, 13);
        //
        // Test length for non-ASCII
        assertFunction("LENGTH('hello na\u00EFve world')", BIGINT, 17);
        assertFunction("LENGTH('\uD801\uDC2Dend')", BIGINT, 4);
        assertFunction("LENGTH('\u4FE1\u5FF5,\u7231,\u5E0C\u671B')", BIGINT, 7);
    }

    @Test
    public void testReplace()
    {
        assertFunction("REPLACE('aaa', 'a', 'aa')", VARCHAR, "aaaaaa");
        assertFunction("REPLACE('abcdefabcdef', 'cd', 'XX')", VARCHAR, "abXXefabXXef");
        assertFunction("REPLACE('abcdefabcdef', 'cd')", VARCHAR, "abefabef");
        assertFunction("REPLACE('123123tech', '123')", VARCHAR, "tech");
        assertFunction("REPLACE('123tech123', '123')", VARCHAR, "tech");
        assertFunction("REPLACE('222tech', '2', '3')", VARCHAR, "333tech");
        assertFunction("REPLACE('0000123', '0')", VARCHAR, "123");
        assertFunction("REPLACE('0000123', '0', ' ')", VARCHAR, "    123");
        assertFunction("REPLACE('foo', '')", VARCHAR, "foo");
        assertFunction("REPLACE('foo', '', '')", VARCHAR, "foo");
        assertFunction("REPLACE('foo', 'foo', '')", VARCHAR, "");
        assertFunction("REPLACE('abc', '', 'xx')", VARCHAR, "xxaxxbxxcxx");
        assertFunction("REPLACE('', '', 'xx')", VARCHAR, "xx");
        assertFunction("REPLACE('', '')", VARCHAR, "");
        assertFunction("REPLACE('', '', '')", VARCHAR, "");
        assertFunction("REPLACE('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', '\u2014')", VARCHAR, "\u4FE1\u5FF5\u2014\u7231\u2014\u5E0C\u671B");
        assertFunction("REPLACE('::\uD801\uDC2D::', ':', '')", VARCHAR, "\uD801\uDC2D");
        assertFunction("REPLACE('\u00D6sterreich', '\u00D6', 'Oe')", VARCHAR, "Oesterreich");

        assertFunction("CAST(REPLACE(utf8(from_hex('CE')), '', 'X') AS VARBINARY)", VARBINARY, new SqlVarbinary(new byte[] {'X', (byte) 0xCE, 'X'}));

        assertFunction("CAST(REPLACE('abc' || utf8(from_hex('CE')), '', 'X') AS VARBINARY)",
                VARBINARY,
                new SqlVarbinary(new byte[] {'X', 'a', 'X', 'b', 'X', 'c', 'X', (byte) 0xCE, 'X'}));

        assertFunction("CAST(REPLACE(utf8(from_hex('CE')) || 'xyz', '', 'X') AS VARBINARY)",
                VARBINARY,
                new SqlVarbinary(new byte[] {'X', (byte) 0xCE, 'X', 'x', 'X', 'y', 'X', 'z', 'X'}));

        assertFunction("CAST(REPLACE('abc' || utf8(from_hex('CE')) || 'xyz', '', 'X') AS VARBINARY)",
                VARBINARY,
                new SqlVarbinary(new byte[] {'X', 'a', 'X', 'b', 'X', 'c', 'X', (byte) 0xCE, 'X', 'x', 'X', 'y', 'X', 'z', 'X'}));
    }

    @Test
    public void testReverse()
    {
        assertFunction("REVERSE('')", VARCHAR, "");
        assertFunction("REVERSE('hello')", VARCHAR, "olleh");
        assertFunction("REVERSE('Quadratically')", VARCHAR, "yllacitardauQ");
        assertFunction("REVERSE('racecar')", VARCHAR, "racecar");
        // Test REVERSE for non-ASCII
        assertFunction("REVERSE('\u4FE1\u5FF5,\u7231,\u5E0C\u671B')", VARCHAR, "\u671B\u5E0C,\u7231,\u5FF5\u4FE1");
        assertFunction("REVERSE('\u00D6sterreich')", VARCHAR, "hcierrets\u00D6");
        assertFunction("REVERSE('na\u00EFve')", VARCHAR, "ev\u00EFan");
        assertFunction("REVERSE('\uD801\uDC2Dend')", VARCHAR, "dne\uD801\uDC2D");

        assertFunction("CAST(REVERSE(utf8(from_hex('CE'))) AS VARBINARY)", VARBINARY, new SqlVarbinary(new byte[] {(byte) 0xCE}));
        assertFunction("CAST(REVERSE('hello' || utf8(from_hex('CE'))) AS VARBINARY)", VARBINARY, new SqlVarbinary(new byte[] {(byte) 0xCE, 'o', 'l', 'l', 'e', 'h'}));
    }

    @Test
    public void testStringPosition()
    {
        testStrPosAndPosition("high", "ig", 2);
        testStrPosAndPosition("high", "igx", 0);
        testStrPosAndPosition("Quadratically", "a", 3);
        testStrPosAndPosition("foobar", "foobar", 1);
        testStrPosAndPosition("foobar", "obar", 3);
        testStrPosAndPosition("zoo!", "!", 4);
        testStrPosAndPosition("x", "", 1);
        testStrPosAndPosition("", "", 1);

        testStrPosAndPosition("\u4FE1\u5FF5,\u7231,\u5E0C\u671B", "\u7231", 4);
        testStrPosAndPosition("\u4FE1\u5FF5,\u7231,\u5E0C\u671B", "\u5E0C\u671B", 6);
        testStrPosAndPosition("\u4FE1\u5FF5,\u7231,\u5E0C\u671B", "nice", 0);

        testStrPosAndPosition(null, "", null);
        testStrPosAndPosition("", null, null);
        testStrPosAndPosition(null, null, null);
    }

    private void testStrPosAndPosition(String string, String substring, Integer expected)
    {
        string = (string == null) ? "NULL" : ("'" + string + "'");
        substring = (substring == null) ? "NULL" : ("'" + substring + "'");

        assertFunction(String.format("STRPOS(%s, %s)", string, substring), BIGINT,  expected);
        assertFunction(String.format("POSITION(%s in %s)", substring, string), BIGINT,  expected);
    }

    @Test
    public void testSubstring()
    {
        assertFunction("SUBSTR('Quadratically', 5)", VARCHAR, "ratically");
        assertFunction("SUBSTR('Quadratically', 50)", VARCHAR, "");
        assertFunction("SUBSTR('Quadratically', -5)", VARCHAR, "cally");
        assertFunction("SUBSTR('Quadratically', -50)", VARCHAR, "");
        assertFunction("SUBSTR('Quadratically', 0)", VARCHAR, "");

        assertFunction("SUBSTR('Quadratically', 5, 6)", VARCHAR, "ratica");
        assertFunction("SUBSTR('Quadratically', 5, 10)", VARCHAR, "ratically");
        assertFunction("SUBSTR('Quadratically', 5, 50)", VARCHAR, "ratically");
        assertFunction("SUBSTR('Quadratically', 50, 10)", VARCHAR, "");
        assertFunction("SUBSTR('Quadratically', -5, 4)", VARCHAR, "call");
        assertFunction("SUBSTR('Quadratically', -5, 40)", VARCHAR, "cally");
        assertFunction("SUBSTR('Quadratically', -50, 4)", VARCHAR, "");
        assertFunction("SUBSTR('Quadratically', 0, 4)", VARCHAR, "");
        assertFunction("SUBSTR('Quadratically', 5, 0)", VARCHAR, "");

        assertFunction("SUBSTRING('Quadratically' FROM 5)", VARCHAR, "ratically");
        assertFunction("SUBSTRING('Quadratically' FROM 50)", VARCHAR, "");
        assertFunction("SUBSTRING('Quadratically' FROM -5)", VARCHAR, "cally");
        assertFunction("SUBSTRING('Quadratically' FROM -50)", VARCHAR, "");
        assertFunction("SUBSTRING('Quadratically' FROM 0)", VARCHAR, "");

        assertFunction("SUBSTRING('Quadratically' FROM 5 FOR 6)", VARCHAR, "ratica");
        assertFunction("SUBSTRING('Quadratically' FROM 5 FOR 50)", VARCHAR, "ratically");
        //
        // Test SUBSTRING for non-ASCII
        assertFunction("SUBSTRING('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' FROM 1 FOR 1)", VARCHAR, "\u4FE1");
        assertFunction("SUBSTRING('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' FROM 3 FOR 5)", VARCHAR, ",\u7231,\u5E0C\u671B");
        assertFunction("SUBSTRING('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' FROM 4)", VARCHAR, "\u7231,\u5E0C\u671B");
        assertFunction("SUBSTRING('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' FROM -2)", VARCHAR, "\u5E0C\u671B");
        assertFunction("SUBSTRING('\uD801\uDC2Dend' FROM 1 FOR 1)", VARCHAR, "\uD801\uDC2D");
        assertFunction("SUBSTRING('\uD801\uDC2Dend' FROM 2 FOR 3)", VARCHAR, "end");
    }

    @Test
    public void testSplit()
    {
        assertFunction("SPLIT('a.b.c', '.')", new ArrayType(VARCHAR), ImmutableList.of("a", "b", "c"));

        assertFunction("SPLIT('ab', '.', 1)", new ArrayType(VARCHAR), ImmutableList.of("ab"));
        assertFunction("SPLIT('a.b', '.', 1)", new ArrayType(VARCHAR), ImmutableList.of("a.b"));
        assertFunction("SPLIT('a.b.c', '.')", new ArrayType(VARCHAR), ImmutableList.of("a", "b", "c"));
        assertFunction("SPLIT('a..b..c', '..')", new ArrayType(VARCHAR), ImmutableList.of("a", "b", "c"));
        assertFunction("SPLIT('a.b.c', '.', 2)", new ArrayType(VARCHAR), ImmutableList.of("a", "b.c"));
        assertFunction("SPLIT('a.b.c', '.', 3)", new ArrayType(VARCHAR), ImmutableList.of("a", "b", "c"));
        assertFunction("SPLIT('a.b.c', '.', 4)", new ArrayType(VARCHAR), ImmutableList.of("a", "b", "c"));
        assertFunction("SPLIT('a.b.c.', '.', 4)", new ArrayType(VARCHAR), ImmutableList.of("a", "b", "c", ""));
        assertFunction("SPLIT('a.b.c.', '.', 3)", new ArrayType(VARCHAR), ImmutableList.of("a", "b", "c."));
        assertFunction("SPLIT('...', '.')", new ArrayType(VARCHAR), ImmutableList.of("", "", "", ""));
        assertFunction("SPLIT('..a...a..', '.')", new ArrayType(VARCHAR), ImmutableList.of("", "", "a", "", "", "a", "", ""));

        // Test SPLIT for non-ASCII
        assertFunction("SPLIT('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 3)", new ArrayType(VARCHAR), ImmutableList.of("\u4FE1\u5FF5", "\u7231", "\u5E0C\u671B"));
        assertFunction("SPLIT('\u8B49\u8BC1\u8A3C', '\u8BC1', 2)", new ArrayType(VARCHAR), ImmutableList.of("\u8B49", "\u8A3C"));

        assertFunction("SPLIT('.a.b.c', '.', 4)", new ArrayType(VARCHAR), ImmutableList.of("", "a", "b", "c"));
        assertFunction("SPLIT('.a.b.c', '.', 3)", new ArrayType(VARCHAR), ImmutableList.of("", "a", "b.c"));
        assertFunction("SPLIT('.a.b.c', '.', 2)", new ArrayType(VARCHAR), ImmutableList.of("", "a.b.c"));
        assertFunction("SPLIT('a..b..c', '.', 3)", new ArrayType(VARCHAR), ImmutableList.of("a", "", "b..c"));
        assertFunction("SPLIT('a.b..', '.', 3)", new ArrayType(VARCHAR), ImmutableList.of("a", "b", "."));

        assertInvalidFunction("SPLIT('a.b.c', '', 1)", "The delimiter may not be the empty string");
        assertInvalidFunction("SPLIT('a.b.c', '.', 0)", "Limit must be positive");
        assertInvalidFunction("SPLIT('a.b.c', '.', -1)", "Limit must be positive");
        assertInvalidFunction("SPLIT('a.b.c', '.', 2147483648)", "Limit is too large");
    }

    @Test
    public void testSplitPart()
    {
        assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 1)", VARCHAR, "abc");
        assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 2)", VARCHAR, "def");
        assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 3)", VARCHAR, "ghi");
        assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 4)", VARCHAR, null);
        assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 99)", VARCHAR, null);
        assertFunction("SPLIT_PART('abc', 'abc', 1)", VARCHAR, "");
        assertFunction("SPLIT_PART('abc', 'abc', 2)", VARCHAR, "");
        assertFunction("SPLIT_PART('abc', 'abc', 3)", VARCHAR, null);
        assertFunction("SPLIT_PART('abc', '-@-', 1)", VARCHAR, "abc");
        assertFunction("SPLIT_PART('abc', '-@-', 2)", VARCHAR, null);
        assertFunction("SPLIT_PART('', 'abc', 1)", VARCHAR, "");
        assertFunction("SPLIT_PART('', '', 1)", VARCHAR, null);
        assertFunction("SPLIT_PART('abc', '', 1)", VARCHAR, "a");
        assertFunction("SPLIT_PART('abc', '', 2)", VARCHAR, "b");
        assertFunction("SPLIT_PART('abc', '', 3)", VARCHAR, "c");
        assertFunction("SPLIT_PART('abc', '', 4)", VARCHAR, null);
        assertFunction("SPLIT_PART('abc', '', 99)", VARCHAR, null);
        assertFunction("SPLIT_PART('abc', 'abcd', 1)", VARCHAR, "abc");
        assertFunction("SPLIT_PART('abc', 'abcd', 2)", VARCHAR, null);
        assertFunction("SPLIT_PART('abc--@--def', '-@-', 1)", VARCHAR, "abc-");
        assertFunction("SPLIT_PART('abc--@--def', '-@-', 2)", VARCHAR, "-def");
        assertFunction("SPLIT_PART('abc-@-@-@-def', '-@-', 1)", VARCHAR, "abc");
        assertFunction("SPLIT_PART('abc-@-@-@-def', '-@-', 2)", VARCHAR, "@");
        assertFunction("SPLIT_PART('abc-@-@-@-def', '-@-', 3)", VARCHAR, "def");
        assertFunction("SPLIT_PART(' ', ' ', 1)", VARCHAR, "");
        assertFunction("SPLIT_PART('abcdddddef', 'dd', 1)", VARCHAR, "abc");
        assertFunction("SPLIT_PART('abcdddddef', 'dd', 2)", VARCHAR, "");
        assertFunction("SPLIT_PART('abcdddddef', 'dd', 3)", VARCHAR, "def");
        assertFunction("SPLIT_PART('a/b/c', '/', 4)", VARCHAR, null);
        assertFunction("SPLIT_PART('a/b/c/', '/', 4)", VARCHAR, "");
        //
        // Test SPLIT_PART for non-ASCII
        assertFunction("SPLIT_PART('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 1)", VARCHAR, "\u4FE1\u5FF5");
        assertFunction("SPLIT_PART('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 2)", VARCHAR, "\u7231");
        assertFunction("SPLIT_PART('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 3)", VARCHAR, "\u5E0C\u671B");
        assertFunction("SPLIT_PART('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 4)", VARCHAR, null);
        assertFunction("SPLIT_PART('\u8B49\u8BC1\u8A3C', '\u8BC1', 1)", VARCHAR, "\u8B49");
        assertFunction("SPLIT_PART('\u8B49\u8BC1\u8A3C', '\u8BC1', 2)", VARCHAR, "\u8A3C");
        assertFunction("SPLIT_PART('\u8B49\u8BC1\u8A3C', '\u8BC1', 3)", VARCHAR, null);

        assertInvalidFunction("SPLIT_PART('abc', '', 0)", "Index must be greater than zero");
        assertInvalidFunction("SPLIT_PART('abc', '', -1)", "Index must be greater than zero");

         assertInvalidFunction("SPLIT_PART(utf8(from_hex('CE')), '', 1)", "Invalid UTF-8 encoding");
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testSplitPartInvalid()
    {
        assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 0)", VARCHAR, "");
    }

    @Test
    public void testLeftTrim()
    {
        assertFunction("LTRIM('')", VARCHAR, "");
        assertFunction("LTRIM('   ')", VARCHAR, "");
        assertFunction("LTRIM('  hello  ')", VARCHAR, "hello  ");
        assertFunction("LTRIM('  hello')", VARCHAR, "hello");
        assertFunction("LTRIM('hello  ')", VARCHAR, "hello  ");
        assertFunction("LTRIM(' hello world ')", VARCHAR, "hello world ");

        assertFunction("LTRIM('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ')", VARCHAR, "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ");
        assertFunction("LTRIM(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ')", VARCHAR, "\u4FE1\u5FF5 \u7231 \u5E0C\u671B ");
        assertFunction("LTRIM('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B')", VARCHAR, "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
        assertFunction("LTRIM(' \u2028 \u4FE1\u5FF5 \u7231 \u5E0C\u671B')", VARCHAR, "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
    }

    @Test
    public void testRightTrim()
    {
        assertFunction("RTRIM('')", VARCHAR, "");
        assertFunction("RTRIM('   ')", VARCHAR, "");
        assertFunction("RTRIM('  hello  ')", VARCHAR, "  hello");
        assertFunction("RTRIM('  hello')", VARCHAR, "  hello");
        assertFunction("RTRIM('hello  ')", VARCHAR, "hello");
        assertFunction("RTRIM(' hello world ')", VARCHAR, " hello world");

        assertFunction("RTRIM('\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 ')", VARCHAR, "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
        assertFunction("RTRIM('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ')", VARCHAR, "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
        assertFunction("RTRIM(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ')", VARCHAR, " \u4FE1\u5FF5 \u7231 \u5E0C\u671B");
        assertFunction("RTRIM('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B')", VARCHAR, "  \u4FE1\u5FF5 \u7231 \u5E0C\u671B");
    }

    @Test
    public void testTrim()
    {
        assertFunction("TRIM('')", VARCHAR, "");
        assertFunction("TRIM('   ')", VARCHAR, "");
        assertFunction("TRIM('  hello  ')", VARCHAR, "hello");
        assertFunction("TRIM('  hello')", VARCHAR, "hello");
        assertFunction("TRIM('hello  ')", VARCHAR, "hello");
        assertFunction("TRIM(' hello world ')", VARCHAR, "hello world");

        assertFunction("TRIM('\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 ')", VARCHAR, "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
        assertFunction("TRIM('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ')", VARCHAR, "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
        assertFunction("TRIM(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ')", VARCHAR, "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
        assertFunction("TRIM('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B')", VARCHAR, "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
        assertFunction("TRIM(' \u2028 \u4FE1\u5FF5 \u7231 \u5E0C\u671B')", VARCHAR, "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
    }

    @Test
    public void testLower()
    {
        assertFunction("LOWER('')", VARCHAR, "");
        assertFunction("LOWER('Hello World')", VARCHAR, "hello world");
        assertFunction("LOWER('WHAT!!')", VARCHAR, "what!!");
        assertFunction("LOWER('\u00D6STERREICH')", VARCHAR, lowerByCodePoint("\u00D6sterreich"));
        assertFunction("LOWER('From\uD801\uDC2DTo')", VARCHAR, lowerByCodePoint("from\uD801\uDC2Dto"));

        assertFunction("CAST(LOWER(utf8(from_hex('CE'))) AS VARBINARY)", VARBINARY, new SqlVarbinary(new byte[] {(byte) 0xCE}));
        assertFunction("CAST(LOWER('HELLO' || utf8(from_hex('CE'))) AS VARBINARY)", VARBINARY, new SqlVarbinary(new byte[] {'h', 'e', 'l', 'l', 'o', (byte) 0xCE}));
        assertFunction("CAST(LOWER(utf8(from_hex('CE')) || 'HELLO') AS VARBINARY)", VARBINARY, new SqlVarbinary(new byte[] {(byte) 0xCE, 'h', 'e', 'l', 'l', 'o'}));
    }

    @Test
    public void testUpper()
    {
        assertFunction("UPPER('')", VARCHAR, "");
        assertFunction("UPPER('Hello World')", VARCHAR, "HELLO WORLD");
        assertFunction("UPPER('what!!')", VARCHAR, "WHAT!!");
        assertFunction("UPPER('\u00D6sterreich')", VARCHAR, upperByCodePoint("\u00D6STERREICH"));
        assertFunction("UPPER('From\uD801\uDC2DTo')", VARCHAR, upperByCodePoint("FROM\uD801\uDC2DTO"));

        assertFunction("CAST(UPPER(utf8(from_hex('CE'))) AS VARBINARY)", VARBINARY, new SqlVarbinary(new byte[] {(byte) 0xCE}));
        assertFunction("CAST(UPPER('hello' || utf8(from_hex('CE'))) AS VARBINARY)", VARBINARY, new SqlVarbinary(new byte[] {'H', 'E', 'L', 'L', 'O', (byte) 0xCE}));
        assertFunction("CAST(UPPER(utf8(from_hex('CE')) || 'hello') AS VARBINARY)", VARBINARY, new SqlVarbinary(new byte[] {(byte) 0xCE, 'H', 'E', 'L', 'L', 'O'}));
    }

    @Test
    public void testNormalize()
    {
        assertFunction("normalize('sch\u00f6n', NFD)", VARCHAR, "scho\u0308n");
        assertFunction("normalize('sch\u00f6n')", VARCHAR, "sch\u00f6n");
        assertFunction("normalize('sch\u00f6n', NFC)", VARCHAR, "sch\u00f6n");
        assertFunction("normalize('sch\u00f6n', NFKD)", VARCHAR, "scho\u0308n");

        assertFunction("normalize('sch\u00f6n', NFKC)", VARCHAR, "sch\u00f6n");
        assertFunction("normalize('\u3231\u3327\u3326\u2162', NFKC)", VARCHAR, "(\u682a)\u30c8\u30f3\u30c9\u30ebIII");
        assertFunction("normalize('\uff8a\uff9d\uff76\uff78\uff76\uff85', NFKC)", VARCHAR, "\u30cf\u30f3\u30ab\u30af\u30ab\u30ca");
    }

    // We do not use String toLowerCase or toUpperCase here because they can do multi character transforms
    // and we want to verify our implementation spec which states we perform case transform code point by
    // code point
    private static String lowerByCodePoint(String string)
    {
        int[] upperCodePoints = string.codePoints().map(Character::toLowerCase).toArray();
        return new String(upperCodePoints, 0, upperCodePoints.length);
    }

    private static String upperByCodePoint(String string)
    {
        int[] upperCodePoints = string.codePoints().map(Character::toUpperCase).toArray();
        return new String(upperCodePoints, 0, upperCodePoints.length);
    }

    @Test
    public void testFromUtf8()
    {
        assertFunction("from_utf8(to_utf8('hello'))", VARCHAR, "hello");
        assertFunction("from_utf8(from_hex('58BF'))", VARCHAR, "X\uFFFD");
        assertFunction("from_utf8(from_hex('58DF'))", VARCHAR, "X\uFFFD");
        assertFunction("from_utf8(from_hex('58F7'))", VARCHAR, "X\uFFFD");

        assertFunction("from_utf8(from_hex('58BF'), '#')", VARCHAR, "X#");
        assertFunction("from_utf8(from_hex('58DF'), 35)", VARCHAR, "X#");
        assertFunction("from_utf8(from_hex('58BF'), '')", VARCHAR, "X");

        assertInvalidFunction("from_utf8(to_utf8('hello'), 'foo')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("from_utf8(to_utf8('hello'), 1114112)", INVALID_FUNCTION_ARGUMENT);
    }
}
