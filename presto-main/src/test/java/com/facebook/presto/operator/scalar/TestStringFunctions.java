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
        functionAssertions.assertFunction("CHR(65)", "A");
        functionAssertions.assertFunction("CHR(65)", "A");
        functionAssertions.assertFunction("CHR(0)", "\0");
    }

    @Test
    public void testConcat()
    {
        functionAssertions.assertFunction("CONCAT('hello', ' world')", "hello world");
        functionAssertions.assertFunction("CONCAT('', '')", "");
        functionAssertions.assertFunction("CONCAT('what', '')", "what");
        functionAssertions.assertFunction("CONCAT('', 'what')", "what");
        functionAssertions.assertFunction("CONCAT(CONCAT('this', ' is'), ' cool')", "this is cool");
        functionAssertions.assertFunction("CONCAT('this', CONCAT(' is', ' cool'))", "this is cool");
    }

    @Test
    public void testLength()
    {
        functionAssertions.assertFunction("LENGTH('')", 0);
        functionAssertions.assertFunction("LENGTH('hello')", 5);
        functionAssertions.assertFunction("LENGTH('Quadratically')", 13);
    }

    @Test
    public void testReplace()
    {
        functionAssertions.assertFunction("REPLACE('aaa', 'a', 'aa')", "aaaaaa");
        functionAssertions.assertFunction("REPLACE('abcdefabcdef', 'cd', 'XX')", "abXXefabXXef");
        functionAssertions.assertFunction("REPLACE('abcdefabcdef', 'cd')", "abefabef");
        functionAssertions.assertFunction("REPLACE('123123tech', '123')", "tech");
        functionAssertions.assertFunction("REPLACE('123tech123', '123')", "tech");
        functionAssertions.assertFunction("REPLACE('222tech', '2', '3')", "333tech");
        functionAssertions.assertFunction("REPLACE('0000123', '0')", "123");
        functionAssertions.assertFunction("REPLACE('0000123', '0', ' ')", "    123");
        functionAssertions.assertFunction("REPLACE('foo', '')", "foo");
        functionAssertions.assertFunction("REPLACE('foo', '', '')", "foo");
        functionAssertions.assertFunction("REPLACE('', '')", "");
        functionAssertions.assertFunction("REPLACE('', '', '')", "");
    }

    @Test
    public void testReverse()
    {
        functionAssertions.assertFunction("REVERSE('')", "");
        functionAssertions.assertFunction("REVERSE('hello')", "olleh");
        functionAssertions.assertFunction("REVERSE('Quadratically')", "yllacitardauQ");
        functionAssertions.assertFunction("REVERSE('racecar')", "racecar");
    }

    @Test
    public void testStringPosition()
    {
        functionAssertions.assertFunction("STRPOS('high', 'ig')", 2);
        functionAssertions.assertFunction("STRPOS('high', 'igx')", 0);
        functionAssertions.assertFunction("STRPOS('Quadratically', 'a')", 3);
        functionAssertions.assertFunction("STRPOS('foobar', 'foobar')", 1);
        functionAssertions.assertFunction("STRPOS('foobar', 'obar')", 3);
        functionAssertions.assertFunction("STRPOS('zoo!', '!')", 4);
        functionAssertions.assertFunction("STRPOS('x', '')", 1);
        functionAssertions.assertFunction("STRPOS('', '')", 1);
    }

    @Test
    public void testSubstring()
    {
        functionAssertions.assertFunction("SUBSTR('Quadratically', 5)", "ratically");
        functionAssertions.assertFunction("SUBSTR('Quadratically', 50)", "");
        functionAssertions.assertFunction("SUBSTR('Quadratically', -5)", "cally");
        functionAssertions.assertFunction("SUBSTR('Quadratically', -50)", "");
        functionAssertions.assertFunction("SUBSTR('Quadratically', 0)", "");

        functionAssertions.assertFunction("SUBSTR('Quadratically', 5, 6)", "ratica");
        functionAssertions.assertFunction("SUBSTR('Quadratically', 5, 10)", "ratically");
        functionAssertions.assertFunction("SUBSTR('Quadratically', 5, 50)", "ratically");
        functionAssertions.assertFunction("SUBSTR('Quadratically', 50, 10)", "");
        functionAssertions.assertFunction("SUBSTR('Quadratically', -5, 4)", "call");
        functionAssertions.assertFunction("SUBSTR('Quadratically', -5, 40)", "cally");
        functionAssertions.assertFunction("SUBSTR('Quadratically', -50, 4)", "");
        functionAssertions.assertFunction("SUBSTR('Quadratically', 0, 4)", "");
        functionAssertions.assertFunction("SUBSTR('Quadratically', 5, 0)", "");

        functionAssertions.assertFunction("SUBSTRING('Quadratically' FROM 5)", "ratically");
        functionAssertions.assertFunction("SUBSTRING('Quadratically' FROM 50)", "");
        functionAssertions.assertFunction("SUBSTRING('Quadratically' FROM -5)", "cally");
        functionAssertions.assertFunction("SUBSTRING('Quadratically' FROM -50)", "");
        functionAssertions.assertFunction("SUBSTRING('Quadratically' FROM 0)", "");

        functionAssertions.assertFunction("SUBSTRING('Quadratically' FROM 5 FOR 6)", "ratica");
        functionAssertions.assertFunction("SUBSTRING('Quadratically' FROM 5 FOR 50)", "ratically");
    }

    @Test
    public void testSplitPart()
    {
        functionAssertions.assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 1)", "abc");
        functionAssertions.assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 2)", "def");
        functionAssertions.assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 3)", "ghi");
        functionAssertions.assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 4)", null);
        functionAssertions.assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 99)", null);
        functionAssertions.assertFunction("SPLIT_PART('abc', 'abc', 1)", "");
        functionAssertions.assertFunction("SPLIT_PART('abc', 'abc', 2)", "");
        functionAssertions.assertFunction("SPLIT_PART('abc', 'abc', 3)", null);
        functionAssertions.assertFunction("SPLIT_PART('abc', '-@-', 1)", "abc");
        functionAssertions.assertFunction("SPLIT_PART('abc', '-@-', 2)", null);
        functionAssertions.assertFunction("SPLIT_PART('', 'abc', 1)", "");
        functionAssertions.assertFunction("SPLIT_PART('', '', 1)", null);
        functionAssertions.assertFunction("SPLIT_PART('abc', '', 1)", "a");
        functionAssertions.assertFunction("SPLIT_PART('abc', '', 2)", "b");
        functionAssertions.assertFunction("SPLIT_PART('abc', '', 3)", "c");
        functionAssertions.assertFunction("SPLIT_PART('abc', '', 4)", null);
        functionAssertions.assertFunction("SPLIT_PART('abc', '', 99)", null);
        functionAssertions.assertFunction("SPLIT_PART('abc', 'abcd', 1)", "abc");
        functionAssertions.assertFunction("SPLIT_PART('abc', 'abcd', 2)", null);
        functionAssertions.assertFunction("SPLIT_PART('abc--@--def', '-@-', 1)", "abc-");
        functionAssertions.assertFunction("SPLIT_PART('abc--@--def', '-@-', 2)", "-def");
        functionAssertions.assertFunction("SPLIT_PART('abc-@-@-@-def', '-@-', 1)", "abc");
        functionAssertions.assertFunction("SPLIT_PART('abc-@-@-@-def', '-@-', 2)", "@");
        functionAssertions.assertFunction("SPLIT_PART('abc-@-@-@-def', '-@-', 3)", "def");
        functionAssertions.assertFunction("SPLIT_PART(' ', ' ', 1)", "");
        functionAssertions.assertFunction("SPLIT_PART('abcdddddef', 'dd', 1)", "abc");
        functionAssertions.assertFunction("SPLIT_PART('abcdddddef', 'dd', 2)", "");
        functionAssertions.assertFunction("SPLIT_PART('abcdddddef', 'dd', 3)", "def");
        functionAssertions.assertFunction("SPLIT_PART('a/b/c', '/', 4)", null);
        functionAssertions.assertFunction("SPLIT_PART('a/b/c/', '/', 4)", "");
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testSplitPartInvalid()
    {
        functionAssertions.assertFunction("SPLIT_PART('abc-@-def-@-ghi', '-@-', 0)", "");
    }

    @Test
    public void testLeftTrim()
    {
        functionAssertions.assertFunction("LTRIM('')", "");
        functionAssertions.assertFunction("LTRIM('   ')", "");
        functionAssertions.assertFunction("LTRIM('  hello  ')", "hello  ");
        functionAssertions.assertFunction("LTRIM('  hello')", "hello");
        functionAssertions.assertFunction("LTRIM('hello  ')", "hello  ");
        functionAssertions.assertFunction("LTRIM(' hello world ')", "hello world ");
    }

    @Test
    public void testRightTrim()
    {
        functionAssertions.assertFunction("RTRIM('')", "");
        functionAssertions.assertFunction("RTRIM('   ')", "");
        functionAssertions.assertFunction("RTRIM('  hello  ')", "  hello");
        functionAssertions.assertFunction("RTRIM('  hello')", "  hello");
        functionAssertions.assertFunction("RTRIM('hello  ')", "hello");
        functionAssertions.assertFunction("RTRIM(' hello world ')", " hello world");
    }

    @Test
    public void testTrim()
    {
        functionAssertions.assertFunction("TRIM('')", "");
        functionAssertions.assertFunction("TRIM('   ')", "");
        functionAssertions.assertFunction("TRIM('  hello  ')", "hello");
        functionAssertions.assertFunction("TRIM('  hello')", "hello");
        functionAssertions.assertFunction("TRIM('hello  ')", "hello");
        functionAssertions.assertFunction("TRIM(' hello world ')", "hello world");
    }

    @Test
    public void testLower()
    {
        functionAssertions.assertFunction("LOWER('')", "");
        functionAssertions.assertFunction("LOWER('Hello World')", "hello world");
        functionAssertions.assertFunction("LOWER('WHAT!!')", "what!!");
    }

    @Test
    public void testUpper()
    {
        functionAssertions.assertFunction("UPPER('')", "");
        functionAssertions.assertFunction("UPPER('Hello World')", "HELLO WORLD");
        functionAssertions.assertFunction("UPPER('what!!')", "WHAT!!");
    }

    private void assertFunction(String projection, Object expected)
    {
        functionAssertions.assertFunction(projection, expected);
    }
}
