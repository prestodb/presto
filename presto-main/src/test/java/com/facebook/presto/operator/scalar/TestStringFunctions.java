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
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;

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
        assertFunction("CHR(65)", createVarcharType(1), "A");
        assertFunction("CHR(9731)", createVarcharType(1), "\u2603");
        assertFunction("CHR(131210)", createVarcharType(1), new String(Character.toChars(131210)));
        assertFunction("CHR(0)", createVarcharType(1), "\0");

        assertInvalidFunction("CHR(-1)", "Not a valid Unicode code point: -1");
        assertInvalidFunction("CHR(1234567)", "Not a valid Unicode code point: 1234567");
        assertInvalidFunction("CHR(8589934592)", "Not a valid Unicode code point: 8589934592");
    }

    @Test
    public void testConcat()
    {
        assertInvalidFunction("CONCAT()", "There must be two or more concatenation arguments");
        assertInvalidFunction("CONCAT('')", "There must be two or more concatenation arguments");
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
        assertFunction("CONCAT('\uD801\uDC2D', 'end', '\uD801\uDC2D')", VARCHAR, "\uD801\uDC2Dend\uD801\uDC2D");
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
        assertFunction("REVERSE('')", createVarcharType(0), "");
        assertFunction("REVERSE('hello')", createVarcharType(5), "olleh");
        assertFunction("REVERSE('Quadratically')", createVarcharType(13), "yllacitardauQ");
        assertFunction("REVERSE('racecar')", createVarcharType(7), "racecar");
        // Test REVERSE for non-ASCII
        assertFunction("REVERSE('\u4FE1\u5FF5,\u7231,\u5E0C\u671B')", createVarcharType(7), "\u671B\u5E0C,\u7231,\u5FF5\u4FE1");
        assertFunction("REVERSE('\u00D6sterreich')", createVarcharType(10), "hcierrets\u00D6");
        assertFunction("REVERSE('na\u00EFve')", createVarcharType(5), "ev\u00EFan");
        assertFunction("REVERSE('\uD801\uDC2Dend')", createVarcharType(4), "dne\uD801\uDC2D");

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
        assertFunction("SUBSTR('Quadratically', 5)", createVarcharType(13), "ratically");
        assertFunction("SUBSTR('Quadratically', 50)", createVarcharType(13), "");
        assertFunction("SUBSTR('Quadratically', -5)", createVarcharType(13), "cally");
        assertFunction("SUBSTR('Quadratically', -50)", createVarcharType(13), "");
        assertFunction("SUBSTR('Quadratically', 0)", createVarcharType(13), "");

        assertFunction("SUBSTR('Quadratically', 5, 6)", createVarcharType(13), "ratica");
        assertFunction("SUBSTR('Quadratically', 5, 10)", createVarcharType(13), "ratically");
        assertFunction("SUBSTR('Quadratically', 5, 50)", createVarcharType(13), "ratically");
        assertFunction("SUBSTR('Quadratically', 50, 10)", createVarcharType(13), "");
        assertFunction("SUBSTR('Quadratically', -5, 4)", createVarcharType(13), "call");
        assertFunction("SUBSTR('Quadratically', -5, 40)", createVarcharType(13), "cally");
        assertFunction("SUBSTR('Quadratically', -50, 4)", createVarcharType(13), "");
        assertFunction("SUBSTR('Quadratically', 0, 4)", createVarcharType(13), "");
        assertFunction("SUBSTR('Quadratically', 5, 0)", createVarcharType(13), "");

        assertFunction("SUBSTRING('Quadratically' FROM 5)", createVarcharType(13), "ratically");
        assertFunction("SUBSTRING('Quadratically' FROM 50)", createVarcharType(13), "");
        assertFunction("SUBSTRING('Quadratically' FROM -5)", createVarcharType(13), "cally");
        assertFunction("SUBSTRING('Quadratically' FROM -50)", createVarcharType(13), "");
        assertFunction("SUBSTRING('Quadratically' FROM 0)", createVarcharType(13), "");

        assertFunction("SUBSTRING('Quadratically' FROM 5 FOR 6)", createVarcharType(13), "ratica");
        assertFunction("SUBSTRING('Quadratically' FROM 5 FOR 50)", createVarcharType(13), "ratically");
        //
        // Test SUBSTRING for non-ASCII
        assertFunction("SUBSTRING('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' FROM 1 FOR 1)", createVarcharType(7), "\u4FE1");
        assertFunction("SUBSTRING('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' FROM 3 FOR 5)", createVarcharType(7), ",\u7231,\u5E0C\u671B");
        assertFunction("SUBSTRING('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' FROM 4)", createVarcharType(7), "\u7231,\u5E0C\u671B");
        assertFunction("SUBSTRING('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' FROM -2)", createVarcharType(7), "\u5E0C\u671B");
        assertFunction("SUBSTRING('\uD801\uDC2Dend' FROM 1 FOR 1)", createVarcharType(4), "\uD801\uDC2D");
        assertFunction("SUBSTRING('\uD801\uDC2Dend' FROM 2 FOR 3)", createVarcharType(4), "end");
    }

    @Test
    public void testSplit()
    {
        assertFunction("SPLIT('a.b.c', '.')", new ArrayType(createVarcharType(5)), ImmutableList.of("a", "b", "c"));

        assertFunction("SPLIT('ab', '.', 1)", new ArrayType(createVarcharType(2)), ImmutableList.of("ab"));
        assertFunction("SPLIT('a.b', '.', 1)", new ArrayType(createVarcharType(3)), ImmutableList.of("a.b"));
        assertFunction("SPLIT('a.b.c', '.')", new ArrayType(createVarcharType(5)), ImmutableList.of("a", "b", "c"));
        assertFunction("SPLIT('a..b..c', '..')", new ArrayType(createVarcharType(7)), ImmutableList.of("a", "b", "c"));
        assertFunction("SPLIT('a.b.c', '.', 2)", new ArrayType(createVarcharType(5)), ImmutableList.of("a", "b.c"));
        assertFunction("SPLIT('a.b.c', '.', 3)", new ArrayType(createVarcharType(5)), ImmutableList.of("a", "b", "c"));
        assertFunction("SPLIT('a.b.c', '.', 4)", new ArrayType(createVarcharType(5)), ImmutableList.of("a", "b", "c"));
        assertFunction("SPLIT('a.b.c.', '.', 4)", new ArrayType(createVarcharType(6)), ImmutableList.of("a", "b", "c", ""));
        assertFunction("SPLIT('a.b.c.', '.', 3)", new ArrayType(createVarcharType(6)), ImmutableList.of("a", "b", "c."));
        assertFunction("SPLIT('...', '.')", new ArrayType(createVarcharType(3)), ImmutableList.of("", "", "", ""));
        assertFunction("SPLIT('..a...a..', '.')", new ArrayType(createVarcharType(9)), ImmutableList.of("", "", "a", "", "", "a", "", ""));

        // Test SPLIT for non-ASCII
        assertFunction("SPLIT('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 3)", new ArrayType(createVarcharType(7)), ImmutableList.of("\u4FE1\u5FF5", "\u7231", "\u5E0C\u671B"));
        assertFunction("SPLIT('\u8B49\u8BC1\u8A3C', '\u8BC1', 2)", new ArrayType(createVarcharType(3)), ImmutableList.of("\u8B49", "\u8A3C"));

        assertFunction("SPLIT('.a.b.c', '.', 4)", new ArrayType(createVarcharType(6)), ImmutableList.of("", "a", "b", "c"));
        assertFunction("SPLIT('.a.b.c', '.', 3)", new ArrayType(createVarcharType(6)), ImmutableList.of("", "a", "b.c"));
        assertFunction("SPLIT('.a.b.c', '.', 2)", new ArrayType(createVarcharType(6)), ImmutableList.of("", "a.b.c"));
        assertFunction("SPLIT('a..b..c', '.', 3)", new ArrayType(createVarcharType(7)), ImmutableList.of("a", "", "b..c"));
        assertFunction("SPLIT('a.b..', '.', 3)", new ArrayType(createVarcharType(5)), ImmutableList.of("a", "b", "."));

        assertInvalidFunction("SPLIT('a.b.c', '', 1)", "The delimiter may not be the empty string");
        assertInvalidFunction("SPLIT('a.b.c', '.', 0)", "Limit must be positive");
        assertInvalidFunction("SPLIT('a.b.c', '.', -1)", "Limit must be positive");
        assertInvalidFunction("SPLIT('a.b.c', '.', 2147483648)", "Limit is too large");
    }

    @Test
    public void testSplitPart()
    {
        assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 1)", createVarcharType(15), "abc");
        assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 2)", createVarcharType(15), "def");
        assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 3)", createVarcharType(15), "ghi");
        assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 4)", createVarcharType(15), null);
        assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 99)", createVarcharType(15), null);
        assertFunction("SPLIT_PART('abc', 'abc', 1)", createVarcharType(3), "");
        assertFunction("SPLIT_PART('abc', 'abc', 2)", createVarcharType(3), "");
        assertFunction("SPLIT_PART('abc', 'abc', 3)", createVarcharType(3), null);
        assertFunction("SPLIT_PART('abc', '-@-', 1)", createVarcharType(3), "abc");
        assertFunction("SPLIT_PART('abc', '-@-', 2)", createVarcharType(3), null);
        assertFunction("SPLIT_PART('', 'abc', 1)", createVarcharType(0), "");
        assertFunction("SPLIT_PART('', '', 1)", createVarcharType(0), null);
        assertFunction("SPLIT_PART('abc', '', 1)", createVarcharType(3), "a");
        assertFunction("SPLIT_PART('abc', '', 2)", createVarcharType(3), "b");
        assertFunction("SPLIT_PART('abc', '', 3)", createVarcharType(3), "c");
        assertFunction("SPLIT_PART('abc', '', 4)", createVarcharType(3), null);
        assertFunction("SPLIT_PART('abc', '', 99)", createVarcharType(3), null);
        assertFunction("SPLIT_PART('abc', 'abcd', 1)", createVarcharType(3), "abc");
        assertFunction("SPLIT_PART('abc', 'abcd', 2)", createVarcharType(3), null);
        assertFunction("SPLIT_PART('abc--@--def', '-@-', 1)", createVarcharType(11), "abc-");
        assertFunction("SPLIT_PART('abc--@--def', '-@-', 2)", createVarcharType(11), "-def");
        assertFunction("SPLIT_PART('abc-@-@-@-def', '-@-', 1)", createVarcharType(13), "abc");
        assertFunction("SPLIT_PART('abc-@-@-@-def', '-@-', 2)", createVarcharType(13), "@");
        assertFunction("SPLIT_PART('abc-@-@-@-def', '-@-', 3)", createVarcharType(13), "def");
        assertFunction("SPLIT_PART(' ', ' ', 1)", createVarcharType(1), "");
        assertFunction("SPLIT_PART('abcdddddef', 'dd', 1)", createVarcharType(10), "abc");
        assertFunction("SPLIT_PART('abcdddddef', 'dd', 2)", createVarcharType(10), "");
        assertFunction("SPLIT_PART('abcdddddef', 'dd', 3)", createVarcharType(10), "def");
        assertFunction("SPLIT_PART('a/b/c', '/', 4)", createVarcharType(5), null);
        assertFunction("SPLIT_PART('a/b/c/', '/', 4)", createVarcharType(6), "");
        //
        // Test SPLIT_PART for non-ASCII
        assertFunction("SPLIT_PART('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 1)", createVarcharType(7), "\u4FE1\u5FF5");
        assertFunction("SPLIT_PART('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 2)", createVarcharType(7), "\u7231");
        assertFunction("SPLIT_PART('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 3)", createVarcharType(7), "\u5E0C\u671B");
        assertFunction("SPLIT_PART('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 4)", createVarcharType(7), null);
        assertFunction("SPLIT_PART('\u8B49\u8BC1\u8A3C', '\u8BC1', 1)", createVarcharType(3), "\u8B49");
        assertFunction("SPLIT_PART('\u8B49\u8BC1\u8A3C', '\u8BC1', 2)", createVarcharType(3), "\u8A3C");
        assertFunction("SPLIT_PART('\u8B49\u8BC1\u8A3C', '\u8BC1', 3)", createVarcharType(3), null);

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
        assertFunction("LTRIM('')", createVarcharType(0), "");
        assertFunction("LTRIM('   ')", createVarcharType(3), "");
        assertFunction("LTRIM('  hello  ')", createVarcharType(9), "hello  ");
        assertFunction("LTRIM('  hello')", createVarcharType(7), "hello");
        assertFunction("LTRIM('hello  ')", createVarcharType(7), "hello  ");
        assertFunction("LTRIM(' hello world ')", createVarcharType(13), "hello world ");

        assertFunction("LTRIM('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ')", createVarcharType(9), "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ");
        assertFunction("LTRIM(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ')", createVarcharType(9), "\u4FE1\u5FF5 \u7231 \u5E0C\u671B ");
        assertFunction("LTRIM('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B')", createVarcharType(9), "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
        assertFunction("LTRIM(' \u2028 \u4FE1\u5FF5 \u7231 \u5E0C\u671B')", createVarcharType(10), "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
    }

    @Test
    public void testRightTrim()
    {
        assertFunction("RTRIM('')", createVarcharType(0), "");
        assertFunction("RTRIM('   ')", createVarcharType(3), "");
        assertFunction("RTRIM('  hello  ')", createVarcharType(9), "  hello");
        assertFunction("RTRIM('  hello')", createVarcharType(7), "  hello");
        assertFunction("RTRIM('hello  ')", createVarcharType(7), "hello");
        assertFunction("RTRIM(' hello world ')", createVarcharType(13), " hello world");

        assertFunction("RTRIM('\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 ')", createVarcharType(10), "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
        assertFunction("RTRIM('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ')", createVarcharType(9), "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
        assertFunction("RTRIM(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ')", createVarcharType(9), " \u4FE1\u5FF5 \u7231 \u5E0C\u671B");
        assertFunction("RTRIM('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B')", createVarcharType(9), "  \u4FE1\u5FF5 \u7231 \u5E0C\u671B");
    }

    @Test
    public void testTrim()
    {
        assertFunction("TRIM('')", createVarcharType(0), "");
        assertFunction("TRIM('   ')", createVarcharType(3), "");
        assertFunction("TRIM('  hello  ')", createVarcharType(9), "hello");
        assertFunction("TRIM('  hello')", createVarcharType(7), "hello");
        assertFunction("TRIM('hello  ')", createVarcharType(7), "hello");
        assertFunction("TRIM(' hello world ')", createVarcharType(13), "hello world");

        assertFunction("TRIM('\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 ')", createVarcharType(10), "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
        assertFunction("TRIM('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ')", createVarcharType(9), "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
        assertFunction("TRIM(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ')", createVarcharType(9), "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
        assertFunction("TRIM('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B')", createVarcharType(9), "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
        assertFunction("TRIM(' \u2028 \u4FE1\u5FF5 \u7231 \u5E0C\u671B')", createVarcharType(10), "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
    }

    @Test
    public void testVarcharToVarcharX()
    {
        assertFunction("LOWER(CAST('HELLO' AS VARCHAR))", createVarcharType(Integer.MAX_VALUE), "hello");
    }

    @Test
    public void testLower()
    {
        assertFunction("LOWER('')", createVarcharType(0), "");
        assertFunction("LOWER('Hello World')", createVarcharType(11), "hello world");
        assertFunction("LOWER('WHAT!!')", createVarcharType(6), "what!!");
        assertFunction("LOWER('\u00D6STERREICH')", createVarcharType(10), lowerByCodePoint("\u00D6sterreich"));
        assertFunction("LOWER('From\uD801\uDC2DTo')", createVarcharType(7), lowerByCodePoint("from\uD801\uDC2Dto"));

        assertFunction("CAST(LOWER(utf8(from_hex('CE'))) AS VARBINARY)", VARBINARY, new SqlVarbinary(new byte[] {(byte) 0xCE}));
        assertFunction("CAST(LOWER('HELLO' || utf8(from_hex('CE'))) AS VARBINARY)", VARBINARY, new SqlVarbinary(new byte[] {'h', 'e', 'l', 'l', 'o', (byte) 0xCE}));
        assertFunction("CAST(LOWER(utf8(from_hex('CE')) || 'HELLO') AS VARBINARY)", VARBINARY, new SqlVarbinary(new byte[] {(byte) 0xCE, 'h', 'e', 'l', 'l', 'o'}));
        assertFunction("CAST(LOWER(utf8(from_hex('C8BAFF'))) AS VARBINARY)", VARBINARY, new SqlVarbinary(new byte[] {(byte) 0xE2, (byte) 0xB1, (byte) 0xA5, (byte) 0xFF}));
    }

    @Test
    public void testUpper()
    {
        assertFunction("UPPER('')", createVarcharType(0), "");
        assertFunction("UPPER('Hello World')", createVarcharType(11), "HELLO WORLD");
        assertFunction("UPPER('what!!')", createVarcharType(6), "WHAT!!");
        assertFunction("UPPER('\u00D6sterreich')", createVarcharType(10), upperByCodePoint("\u00D6STERREICH"));
        assertFunction("UPPER('From\uD801\uDC2DTo')", createVarcharType(7), upperByCodePoint("FROM\uD801\uDC2DTO"));

        assertFunction("CAST(UPPER(utf8(from_hex('CE'))) AS VARBINARY)", VARBINARY, new SqlVarbinary(new byte[] {(byte) 0xCE}));
        assertFunction("CAST(UPPER('hello' || utf8(from_hex('CE'))) AS VARBINARY)", VARBINARY, new SqlVarbinary(new byte[] {'H', 'E', 'L', 'L', 'O', (byte) 0xCE}));
        assertFunction("CAST(UPPER(utf8(from_hex('CE')) || 'hello') AS VARBINARY)", VARBINARY, new SqlVarbinary(new byte[] {(byte) 0xCE, 'H', 'E', 'L', 'L', 'O'}));
    }

    @Test
    public void testLeftPad()
    {
        assertFunction("LPAD('text', 5, 'x')", VARCHAR, "xtext");
        assertFunction("LPAD('text', 4, 'x')", VARCHAR, "text");

        assertFunction("LPAD('text', 6, 'xy')", VARCHAR, "xytext");
        assertFunction("LPAD('text', 7, 'xy')", VARCHAR, "xyxtext");
        assertFunction("LPAD('text', 9, 'xyz')", VARCHAR, "xyzxytext");

        assertFunction("LPAD('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 10, '\u671B')", VARCHAR, "\u671B\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ");
        assertFunction("LPAD('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 11, '\u671B')", VARCHAR, "\u671B\u671B\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ");
        assertFunction("LPAD('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 12, '\u5E0C\u671B')", VARCHAR, "\u5E0C\u671B\u5E0C\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ");
        assertFunction("LPAD('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 13, '\u5E0C\u671B')", VARCHAR, "\u5E0C\u671B\u5E0C\u671B\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ");

        assertFunction("LPAD('', 3, 'a')", VARCHAR, "aaa");
        assertFunction("LPAD('abc', 0, 'e')", VARCHAR, "");

        // truncation
        assertFunction("LPAD('text', 3, 'xy')", VARCHAR, "tex");
        assertFunction("LPAD('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 5, '\u671B')", VARCHAR, "\u4FE1\u5FF5 \u7231 ");

        // failure modes
        assertInvalidFunction("LPAD('abc', 3, '')", "Padding string must not be empty");

        // invalid target lengths
        long maxSize = Integer.MAX_VALUE;
        assertInvalidFunction("LPAD('abc', -1, 'foo')", "Target length must be in the range [0.." + maxSize + "]");
        assertInvalidFunction("LPAD('abc', " + (maxSize + 1) + ", '')", "Target length must be in the range [0.." + maxSize + "]");
    }

    @Test
    public void testRightPad()
    {
        assertFunction("RPAD('text', 5, 'x')", VARCHAR, "textx");
        assertFunction("RPAD('text', 4, 'x')", VARCHAR, "text");

        assertFunction("RPAD('text', 6, 'xy')", VARCHAR, "textxy");
        assertFunction("RPAD('text', 7, 'xy')", VARCHAR, "textxyx");
        assertFunction("RPAD('text', 9, 'xyz')", VARCHAR, "textxyzxy");

        assertFunction("RPAD('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 10, '\u671B')", VARCHAR, "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  \u671B");
        assertFunction("RPAD('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 11, '\u671B')", VARCHAR, "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  \u671B\u671B");
        assertFunction("RPAD('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 12, '\u5E0C\u671B')", VARCHAR, "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  \u5E0C\u671B\u5E0C");
        assertFunction("RPAD('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 13, '\u5E0C\u671B')", VARCHAR, "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  \u5E0C\u671B\u5E0C\u671B");

        assertFunction("RPAD('', 3, 'a')", VARCHAR, "aaa");
        assertFunction("RPAD('abc', 0, 'e')", VARCHAR, "");

        // truncation
        assertFunction("RPAD('text', 3, 'xy')", VARCHAR, "tex");
        assertFunction("RPAD('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 5, '\u671B')", VARCHAR, "\u4FE1\u5FF5 \u7231 ");

        // failure modes
        assertInvalidFunction("RPAD('abc', 3, '')", "Padding string must not be empty");

        // invalid target lengths
        long maxSize = Integer.MAX_VALUE;
        assertInvalidFunction("RPAD('abc', -1, 'foo')", "Target length must be in the range [0.." + maxSize + "]");
        assertInvalidFunction("RPAD('abc', " + (maxSize + 1) + ", '')", "Target length must be in the range [0.." + maxSize + "]");
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
