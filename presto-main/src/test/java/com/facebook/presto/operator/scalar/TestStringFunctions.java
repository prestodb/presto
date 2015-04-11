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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestStringFunctions
{
    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        functionAssertions = new FunctionAssertions();
        functionAssertions.addScalarFunctions(TestStringFunctions.class);
    }

    @ScalarFunction("utf8")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice convertBinaryToVarchar(@SqlType(StandardTypes.VARBINARY) Slice binary)
    {
        return binary;
    }

    @Test
    public void testChr()
    {
        assertFunction("CHR(65)", "A");
        assertFunction("CHR(9731)", "\u2603");
        assertFunction("CHR(131210)", new String(Character.toChars(131210)));
        assertFunction("CHR(0)", "\0");

        assertInvalidFunction("CHR(-1)", "Not a valid Unicode code point: -1");
        assertInvalidFunction("CHR(1234567)", "Not a valid Unicode code point: 1234567");
        assertInvalidFunction("CHR(8589934592)", "Not a valid Unicode code point: 8589934592");
    }

    @Test
    public void testConcat()
    {
        assertFunction("CONCAT('hello', ' world')", "hello world");
        assertFunction("CONCAT('', '')", "");
        assertFunction("CONCAT('what', '')", "what");
        assertFunction("CONCAT('', 'what')", "what");
        assertFunction("CONCAT(CONCAT('this', ' is'), ' cool')", "this is cool");
        assertFunction("CONCAT('this', CONCAT(' is', ' cool'))", "this is cool");
        //
        // Test concat for non-ASCII
        assertFunction("CONCAT('hello na\u00EFve', ' world')", "hello na\u00EFve world");
        assertFunction("CONCAT('\uD801\uDC2D', 'end')", "\uD801\uDC2Dend");
        assertFunction("CONCAT(CONCAT('\u4FE1\u5FF5', ',\u7231'), ',\u5E0C\u671B')", "\u4FE1\u5FF5,\u7231,\u5E0C\u671B");
    }

    @Test
    public void testLength()
    {
        assertFunction("LENGTH('')", 0);
        assertFunction("LENGTH('hello')", 5);
        assertFunction("LENGTH('Quadratically')", 13);
        //
        // Test length for non-ASCII
        assertFunction("LENGTH('hello na\u00EFve world')", 17);
        assertFunction("LENGTH('\uD801\uDC2Dend')", 4);
        assertFunction("LENGTH('\u4FE1\u5FF5,\u7231,\u5E0C\u671B')", 7);
    }

    @Test
    public void testReplace()
    {
        assertFunction("REPLACE('aaa', 'a', 'aa')", "aaaaaa");
        assertFunction("REPLACE('abcdefabcdef', 'cd', 'XX')", "abXXefabXXef");
        assertFunction("REPLACE('abcdefabcdef', 'cd')", "abefabef");
        assertFunction("REPLACE('123123tech', '123')", "tech");
        assertFunction("REPLACE('123tech123', '123')", "tech");
        assertFunction("REPLACE('222tech', '2', '3')", "333tech");
        assertFunction("REPLACE('0000123', '0')", "123");
        assertFunction("REPLACE('0000123', '0', ' ')", "    123");
        assertFunction("REPLACE('foo', '')", "foo");
        assertFunction("REPLACE('foo', '', '')", "foo");
        assertFunction("REPLACE('foo', 'foo', '')", "");
        assertFunction("REPLACE('abc', '', 'xx')", "xxaxxbxxcxx");
        assertFunction("REPLACE('', '', 'xx')", "xx");
        assertFunction("REPLACE('', '')", "");
        assertFunction("REPLACE('', '', '')", "");
        assertFunction("REPLACE('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', '\u2014')", "\u4FE1\u5FF5\u2014\u7231\u2014\u5E0C\u671B");
        assertFunction("REPLACE('::\uD801\uDC2D::', ':', '')", "\uD801\uDC2D");
        assertFunction("REPLACE('\u00D6sterreich', '\u00D6', 'Oe')", "Oesterreich");
    }

    @Test
    public void testReverse()
    {
        assertFunction("REVERSE('')", "");
        assertFunction("REVERSE('hello')", "olleh");
        assertFunction("REVERSE('Quadratically')", "yllacitardauQ");
        assertFunction("REVERSE('racecar')", "racecar");
        // Test REVERSE for non-ASCII
        assertFunction("REVERSE('\u4FE1\u5FF5,\u7231,\u5E0C\u671B')", "\u671B\u5E0C,\u7231,\u5FF5\u4FE1");
        assertFunction("REVERSE('\u00D6sterreich')", "hcierrets\u00D6");
        assertFunction("REVERSE('na\u00EFve')", "ev\u00EFan");
        assertFunction("REVERSE('\uD801\uDC2Dend')", "dne\uD801\uDC2D");

        assertInvalidFunction("REVERSE(utf8(from_hex('CE')))", "Invalid utf8 encoding");
        assertInvalidFunction("REVERSE(utf8(from_hex('68656C6C6FCE')))", "Invalid utf8 encoding");
    }

    @Test
    public void testStringPosition()
    {
        assertFunction("STRPOS('high', 'ig')", 2);
        assertFunction("STRPOS('high', 'igx')", 0);
        assertFunction("STRPOS('Quadratically', 'a')", 3);
        assertFunction("STRPOS('foobar', 'foobar')", 1);
        assertFunction("STRPOS('foobar', 'obar')", 3);
        assertFunction("STRPOS('zoo!', '!')", 4);
        assertFunction("STRPOS('x', '')", 1);
        assertFunction("STRPOS('', '')", 1);

        assertFunction("STRPOS('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u7231')", 4);
        assertFunction("STRPOS('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u5E0C\u671B')", 6);
        assertFunction("STRPOS('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', 'nice')", 0);
    }

    @Test
    public void testSubstring()
    {
        assertFunction("SUBSTR('Quadratically', 5)", "ratically");
        assertFunction("SUBSTR('Quadratically', 50)", "");
        assertFunction("SUBSTR('Quadratically', -5)", "cally");
        assertFunction("SUBSTR('Quadratically', -50)", "");
        assertFunction("SUBSTR('Quadratically', 0)", "");

        assertFunction("SUBSTR('Quadratically', 5, 6)", "ratica");
        assertFunction("SUBSTR('Quadratically', 5, 10)", "ratically");
        assertFunction("SUBSTR('Quadratically', 5, 50)", "ratically");
        assertFunction("SUBSTR('Quadratically', 50, 10)", "");
        assertFunction("SUBSTR('Quadratically', -5, 4)", "call");
        assertFunction("SUBSTR('Quadratically', -5, 40)", "cally");
        assertFunction("SUBSTR('Quadratically', -50, 4)", "");
        assertFunction("SUBSTR('Quadratically', 0, 4)", "");
        assertFunction("SUBSTR('Quadratically', 5, 0)", "");

        assertFunction("SUBSTRING('Quadratically' FROM 5)", "ratically");
        assertFunction("SUBSTRING('Quadratically' FROM 50)", "");
        assertFunction("SUBSTRING('Quadratically' FROM -5)", "cally");
        assertFunction("SUBSTRING('Quadratically' FROM -50)", "");
        assertFunction("SUBSTRING('Quadratically' FROM 0)", "");

        assertFunction("SUBSTRING('Quadratically' FROM 5 FOR 6)", "ratica");
        assertFunction("SUBSTRING('Quadratically' FROM 5 FOR 50)", "ratically");
        //
        // Test SUBSTRING for non-ASCII
        assertFunction("SUBSTRING('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' FROM 1 FOR 1)", "\u4FE1");
        assertFunction("SUBSTRING('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' FROM 3 FOR 5)", ",\u7231,\u5E0C\u671B");
        assertFunction("SUBSTRING('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' FROM 4)", "\u7231,\u5E0C\u671B");
        assertFunction("SUBSTRING('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' FROM -2)", "\u5E0C\u671B");
        assertFunction("SUBSTRING('\uD801\uDC2Dend' FROM 1 FOR 1)", "\uD801\uDC2D");
        assertFunction("SUBSTRING('\uD801\uDC2Dend' FROM 2 FOR 3)", "end");
    }

    @Test
    public void testSplit()
    {
        assertFunction("SPLIT('a.b.c', '.')", ImmutableList.of("a", "b", "c"));
        assertFunction("SPLIT('a..b..c', '..')", ImmutableList.of("a", "b", "c"));
        assertFunction("SPLIT('a.b.c', '.', 2)", ImmutableList.of("a", "b.c"));
        assertFunction("SPLIT('a.b.c', '.', 3)", ImmutableList.of("a", "b", "c"));
        assertFunction("SPLIT('a.b.c', '.', 4)", ImmutableList.of("a", "b", "c"));
        assertFunction("SPLIT('a.b.c.', '.', 4)", ImmutableList.of("a", "b", "c"));
        assertFunction("SPLIT('a.b.c.', '.', 3)", ImmutableList.of("a", "b", "c."));
        //
        // Test SPLIT for non-ASCII
        assertFunction("SPLIT('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 3)", ImmutableList.of("\u4FE1\u5FF5", "\u7231", "\u5E0C\u671B"));
        assertFunction("SPLIT('\u8B49\u8BC1\u8A3C', '\u8BC1', 2)", ImmutableList.of("\u8B49", "\u8A3C"));

        assertFunction("SPLIT('.a.b.c', '.', 4)", ImmutableList.of("a", "b", "c"));
        assertFunction("SPLIT('.a.b.c', '.', 3)", ImmutableList.of("a", "b", "c"));
        assertFunction("SPLIT('.a.b.c', '.', 2)", ImmutableList.of("a", "b.c"));
        assertFunction("SPLIT('a..b..c', '.', 3)", ImmutableList.of("a", "b", "c"));

        assertInvalidFunction("SPLIT('a.b.c', '', 1)", "The delimiter may not be the empty string");
        assertInvalidFunction("SPLIT('a.b.c', '.', 0)", "Limit must be positive");
        assertInvalidFunction("SPLIT('a.b.c', '.', -1)", "Limit must be positive");
        assertInvalidFunction("SPLIT('a.b.c', '.', 2147483648)", "Limit is too large");
    }

    @Test
    public void testSplitPart()
    {
        assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 1)", "abc");
        assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 2)", "def");
        assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 3)", "ghi");
        assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 4)", null);
        assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 99)", null);
        assertFunction("SPLIT_PART('abc', 'abc', 1)", "");
        assertFunction("SPLIT_PART('abc', 'abc', 2)", "");
        assertFunction("SPLIT_PART('abc', 'abc', 3)", null);
        assertFunction("SPLIT_PART('abc', '-@-', 1)", "abc");
        assertFunction("SPLIT_PART('abc', '-@-', 2)", null);
        assertFunction("SPLIT_PART('', 'abc', 1)", "");
        assertFunction("SPLIT_PART('', '', 1)", null);
        assertFunction("SPLIT_PART('abc', '', 1)", "a");
        assertFunction("SPLIT_PART('abc', '', 2)", "b");
        assertFunction("SPLIT_PART('abc', '', 3)", "c");
        assertFunction("SPLIT_PART('abc', '', 4)", null);
        assertFunction("SPLIT_PART('abc', '', 99)", null);
        assertFunction("SPLIT_PART('abc', 'abcd', 1)", "abc");
        assertFunction("SPLIT_PART('abc', 'abcd', 2)", null);
        assertFunction("SPLIT_PART('abc--@--def', '-@-', 1)", "abc-");
        assertFunction("SPLIT_PART('abc--@--def', '-@-', 2)", "-def");
        assertFunction("SPLIT_PART('abc-@-@-@-def', '-@-', 1)", "abc");
        assertFunction("SPLIT_PART('abc-@-@-@-def', '-@-', 2)", "@");
        assertFunction("SPLIT_PART('abc-@-@-@-def', '-@-', 3)", "def");
        assertFunction("SPLIT_PART(' ', ' ', 1)", "");
        assertFunction("SPLIT_PART('abcdddddef', 'dd', 1)", "abc");
        assertFunction("SPLIT_PART('abcdddddef', 'dd', 2)", "");
        assertFunction("SPLIT_PART('abcdddddef', 'dd', 3)", "def");
        assertFunction("SPLIT_PART('a/b/c', '/', 4)", null);
        assertFunction("SPLIT_PART('a/b/c/', '/', 4)", "");
        //
        // Test SPLIT_PART for non-ASCII
        assertFunction("SPLIT_PART('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 1)", "\u4FE1\u5FF5");
        assertFunction("SPLIT_PART('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 2)", "\u7231");
        assertFunction("SPLIT_PART('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 3)", "\u5E0C\u671B");
        assertFunction("SPLIT_PART('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 4)", null);
        assertFunction("SPLIT_PART('\u8B49\u8BC1\u8A3C', '\u8BC1', 1)", "\u8B49");
        assertFunction("SPLIT_PART('\u8B49\u8BC1\u8A3C', '\u8BC1', 2)", "\u8A3C");
        assertFunction("SPLIT_PART('\u8B49\u8BC1\u8A3C', '\u8BC1', 3)", null);

        assertInvalidFunction("SPLIT_PART('abc', '', 0)", "Index must be greater than zero");
        assertInvalidFunction("SPLIT_PART('abc', '', -1)", "Index must be greater than zero");

        assertInvalidFunction("SPLIT_PART(utf8(from_hex('CE')), '', 1)", "Invalid utf8 encoding");
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testSplitPartInvalid()
    {
        assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 0)", "");
    }

    @Test
    public void testLeftTrim()
    {
        assertFunction("LTRIM('')", "");
        assertFunction("LTRIM('   ')", "");
        assertFunction("LTRIM('  hello  ')", "hello  ");
        assertFunction("LTRIM('  hello')", "hello");
        assertFunction("LTRIM('hello  ')", "hello  ");
        assertFunction("LTRIM(' hello world ')", "hello world ");

        assertFunction("LTRIM('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ')", "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ");
        assertFunction("LTRIM(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ')", "\u4FE1\u5FF5 \u7231 \u5E0C\u671B ");
        assertFunction("LTRIM('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B')", "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
    }

    @Test
    public void testRightTrim()
    {
        assertFunction("RTRIM('')", "");
        assertFunction("RTRIM('   ')", "");
        assertFunction("RTRIM('  hello  ')", "  hello");
        assertFunction("RTRIM('  hello')", "  hello");
        assertFunction("RTRIM('hello  ')", "hello");
        assertFunction("RTRIM(' hello world ')", " hello world");

        assertFunction("RTRIM('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ')", "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
        assertFunction("RTRIM(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ')", " \u4FE1\u5FF5 \u7231 \u5E0C\u671B");
        assertFunction("RTRIM('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B')", "  \u4FE1\u5FF5 \u7231 \u5E0C\u671B");
    }

    @Test
    public void testTrim()
    {
        assertFunction("TRIM('')", "");
        assertFunction("TRIM('   ')", "");
        assertFunction("TRIM('  hello  ')", "hello");
        assertFunction("TRIM('  hello')", "hello");
        assertFunction("TRIM('hello  ')", "hello");
        assertFunction("TRIM(' hello world ')", "hello world");

        assertFunction("TRIM('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ')", "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
        assertFunction("TRIM(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ')", "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
        assertFunction("TRIM('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B')", "\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
    }

    @Test
    public void testLower()
    {
        assertFunction("LOWER('')", "");
        assertFunction("LOWER('Hello World')", "hello world");
        assertFunction("LOWER('WHAT!!')", "what!!");
        //
        // LOWER only supports A-Z
        assertFunction("LOWER('\u00D6STERREICH')", "\u00D6sterreich");
        assertFunction("LOWER('From\uD801\uDC2DTo')", "from\uD801\uDC2Dto");
    }

    @Test
    public void testUpper()
    {
        assertFunction("UPPER('')", "");
        assertFunction("UPPER('Hello World')", "HELLO WORLD");
        assertFunction("UPPER('what!!')", "WHAT!!");
        //
        // UPPER only supports A-Z
        assertFunction("UPPER('\u00D6sterreich')", "\u00D6STERREICH");
        assertFunction("UPPER('From\uD801\uDC2DTo')", "FROM\uD801\uDC2DTO");
    }

    private void assertFunction(String projection, Object expected)
    {
        functionAssertions.assertFunction(projection, expected);
    }

    private void assertInvalidFunction(String projection, String message)
    {
        try {
            assertFunction(projection, null);
            fail("Expected to throw an INVALID_FUNCTION_ARGUMENT exception with message " + message);
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), INVALID_FUNCTION_ARGUMENT.toErrorCode());
            assertEquals(e.getMessage(), message);
        }
    }
}
