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

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestStringFunctions
{
    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        functionAssertions = new FunctionAssertions();
    }

    @Test
    public void testChr()
    {
        assertFunction("CHR(65)", "A");
        assertFunction("CHR(9731)", "\u2603");
        assertFunction("CHR(131210)", new String(Character.toChars(131210)));
        assertFunction("CHR(0)", "\0");
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
    }

    @Test
    public void testLength()
    {
        assertFunction("LENGTH('')", 0);
        assertFunction("LENGTH('hello')", 5);
        assertFunction("LENGTH('Quadratically')", 13);
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
        assertFunction("REPLACE('', '')", "");
        assertFunction("REPLACE('', '', '')", "");
    }

    @Test
    public void testReverse()
    {
        assertFunction("REVERSE('')", "");
        assertFunction("REVERSE('hello')", "olleh");
        assertFunction("REVERSE('Quadratically')", "yllacitardauQ");
        assertFunction("REVERSE('racecar')", "racecar");
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
    }

    @Test
    public void testLower()
    {
        assertFunction("LOWER('')", "");
        assertFunction("LOWER('Hello World')", "hello world");
        assertFunction("LOWER('WHAT!!')", "what!!");
    }

    @Test
    public void testUpper()
    {
        assertFunction("UPPER('')", "");
        assertFunction("UPPER('Hello World')", "HELLO WORLD");
        assertFunction("UPPER('what!!')", "WHAT!!");
    }

    private void assertFunction(String projection, Object expected)
    {
        functionAssertions.assertFunction(projection, expected);
    }
}
