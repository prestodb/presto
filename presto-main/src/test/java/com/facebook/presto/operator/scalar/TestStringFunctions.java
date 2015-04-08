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

import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestStringFunctions
        extends AbstractTestFunctions
{
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
    }

    @Test
    public void testLength()
    {
        assertFunction("LENGTH('')", BIGINT, 0);
        assertFunction("LENGTH('hello')", BIGINT, 5);
        assertFunction("LENGTH('Quadratically')", BIGINT, 13);
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
        assertFunction("REPLACE('', '')", VARCHAR, "");
        assertFunction("REPLACE('', '', '')", VARCHAR, "");
    }

    @Test
    public void testReverse()
    {
        assertFunction("REVERSE('')", VARCHAR, "");
        assertFunction("REVERSE('hello')", VARCHAR, "olleh");
        assertFunction("REVERSE('Quadratically')", VARCHAR, "yllacitardauQ");
        assertFunction("REVERSE('racecar')", VARCHAR, "racecar");
    }

    @Test
    public void testStringPosition()
    {
        assertFunction("STRPOS('high', 'ig')", BIGINT, 2);
        assertFunction("STRPOS('high', 'igx')", BIGINT, 0);
        assertFunction("STRPOS('Quadratically', 'a')", BIGINT, 3);
        assertFunction("STRPOS('foobar', 'foobar')", BIGINT, 1);
        assertFunction("STRPOS('foobar', 'obar')", BIGINT, 3);
        assertFunction("STRPOS('zoo!', '!')", BIGINT, 4);
        assertFunction("STRPOS('x', '')", BIGINT, 1);
        assertFunction("STRPOS('', '')", BIGINT, 1);
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
    }

    @Test
    public void testSplit()
    {
        assertFunction("SPLIT('a.b.c', '.')", new ArrayType(VARCHAR), ImmutableList.of("a", "b", "c"));
        assertFunction("SPLIT('a..b..c', '..')", new ArrayType(VARCHAR), ImmutableList.of("a", "b", "c"));
        assertFunction("SPLIT('a.b.c', '.', 2)", new ArrayType(VARCHAR), ImmutableList.of("a", "b.c"));
        assertFunction("SPLIT('a.b.c', '.', 3)", new ArrayType(VARCHAR), ImmutableList.of("a", "b", "c"));
        assertFunction("SPLIT('a.b.c', '.', 4)", new ArrayType(VARCHAR), ImmutableList.of("a", "b", "c"));
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

        assertInvalidFunction("SPLIT_PART('abc', '', 0)", "Index must be greater than zero");
        assertInvalidFunction("SPLIT_PART('abc', '', -1)", "Index must be greater than zero");
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
    }

    @Test
    public void testLower()
    {
        assertFunction("LOWER('')", VARCHAR, "");
        assertFunction("LOWER('Hello World')", VARCHAR, "hello world");
        assertFunction("LOWER('WHAT!!')", VARCHAR, "what!!");
    }

    @Test
    public void testUpper()
    {
        assertFunction("UPPER('')", VARCHAR, "");
        assertFunction("UPPER('Hello World')", VARCHAR, "HELLO WORLD");
        assertFunction("UPPER('what!!')", VARCHAR, "WHAT!!");
    }
}
