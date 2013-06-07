package com.facebook.presto.operator.scalar;

import org.testng.annotations.Test;

import static com.facebook.presto.operator.scalar.FunctionAssertions.assertFunction;

public class TestStringFunctions
{
    @Test
    public void testChr()
    {
        assertFunction("CHR(65)", "A");
        assertFunction("CHR(65)", "A");
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

    private void validateUrlExtract(String url, String host, String path, String query, String fragment, String protocol, String authority, String file, String userinfo)
    {
        assertFunction("url_extract('" + url + "', 'host')", host);
        assertFunction("url_extract('" + url + "', 'path')", path);
        assertFunction("url_extract('" + url + "', 'query')", query);
        assertFunction("url_extract('" + url + "', 'ref')", fragment);
        assertFunction("url_extract('" + url + "', 'fragment')", fragment);
        assertFunction("url_extract('" + url + "', 'protocol')", protocol);
        assertFunction("url_extract('" + url + "', 'authority')", authority);
        assertFunction("url_extract('" + url + "', 'file')", file);
        assertFunction("url_extract('" + url + "', 'userinfo')", userinfo);
    }

    @Test
    public void testUrlExtract()
    {
        validateUrlExtract("http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1", "facebook.com", "/path1/p.php", "k1=v1&k2=v2", "Ref1", "http", "facebook.com", "/path1/p.php?k1=v1&k2=v2", "");
        validateUrlExtract("http://facebook.com/path1/p.php?", "facebook.com", "/path1/p.php", "", "", "http", "facebook.com", "/path1/p.php?", "");
        validateUrlExtract("http://facebook.com/path1/p.php", "facebook.com", "/path1/p.php", "", "", "http", "facebook.com", "/path1/p.php", "");
        validateUrlExtract("http://facebook.com:8080/path1/p.php?k1=v1&k2=v2#Ref1", "facebook.com", "/path1/p.php", "k1=v1&k2=v2", "Ref1", "http", "facebook.com:8080", "/path1/p.php?k1=v1&k2=v2", "");
        validateUrlExtract("https://username@example.com", "example.com", "", "", "", "https", "username@example.com", "", "username");
        validateUrlExtract("https://username:password@example.com", "example.com", "", "", "", "https", "username:password@example.com", "", "username:password");
        validateUrlExtract("mailto:someone@example.com,someoneelse@example.com", "", "", "", "", "mailto", "", "", "");
        validateUrlExtract("foo", "", "foo", "", "", "", "", "foo", "");
        validateUrlExtract("http://facebook.com/^", null, null, null, null, null, null, null, null);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testUrlExtractInvalid()
    {
        assertFunction("url_extract('http://facebook.com', 'blah')", "");
    }

    @Test
    public void testUrlExtractParam()
    {
        assertFunction("url_extract_param('http://facebook.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k1')", "v1");
        assertFunction("url_extract_param('http://facebook.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k2')", "v2");
        assertFunction("url_extract_param('http://facebook.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k3')", "");
        assertFunction("url_extract_param('http://facebook.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k4')", "");
        assertFunction("url_extract_param('http://facebook.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k5')", null);
        assertFunction("url_extract_param('http://facebook.com/path1/p.php?k1=v1&k1=v2&k1&k1#Ref1', 'k1')", "v1");
        assertFunction("url_extract_param('http://facebook.com/path1/p.php?k1&k1=v1&k1&k1#Ref1', 'k1')", "");
        assertFunction("url_extract_param('http://facebook.com/path1/p.php?k=a=b=c&x=y#Ref1', 'k')", "a=b=c");
        assertFunction("url_extract_param('foo', 'k1')", null);
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
}
