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

import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestUrlFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testUrlExtract()
    {
        validateUrlExtract("http://example.com/path1/p.php?k1=v1&k2=v2#Ref1", "http", "example.com", null, "/path1/p.php", "k1=v1&k2=v2", "Ref1");
        validateUrlExtract("http://example.com/path1/p.php?", "http", "example.com", null, "/path1/p.php", "", "");
        validateUrlExtract("http://example.com/path1/p.php", "http", "example.com", null, "/path1/p.php", "", "");
        validateUrlExtract("http://example.com:8080/path1/p.php?k1=v1&k2=v2#Ref1", "http", "example.com", 8080L, "/path1/p.php", "k1=v1&k2=v2", "Ref1");
        validateUrlExtract("https://username@example.com", "https", "example.com", null, "", "", "");
        validateUrlExtract("https://username:password@example.com", "https", "example.com", null, "", "", "");
        validateUrlExtract("mailto:test@example.com", "mailto", "", null, "", "", "");
        validateUrlExtract("foo", "", "", null, "foo", "", "");
        validateUrlExtract("http://example.com/^", null, null, null, null, null, null);
    }

    @Test
    public void testUrlExtractParameter()
    {
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k1')", VARCHAR, "v1");
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k2')", VARCHAR, "v2");
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k3')", VARCHAR, "");
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k4')", VARCHAR, "");
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k5')", VARCHAR, null);
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k1=v2&k1&k1#Ref1', 'k1')", VARCHAR, "v1");
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k1&k1=v1&k1&k1#Ref1', 'k1')", VARCHAR, "");
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k=a=b=c&x=y#Ref1', 'k')", VARCHAR, "a=b=c");
        assertFunction("url_extract_parameter('foo', 'k1')", VARCHAR, null);
    }

    @Test
    public void testUrlEncode()
    {
        assertFunction("url_encode('http://test')", VARCHAR, "http%3A%2F%2Ftest");
        assertFunction("url_encode('http://test?a=b&c=d')", VARCHAR, "http%3A%2F%2Ftest%3Fa%3Db%26c%3Dd");
        assertFunction("url_encode('http://\u30c6\u30b9\u30c8')", VARCHAR, "http%3A%2F%2F%E3%83%86%E3%82%B9%E3%83%88");
        assertFunction("url_encode('~@:.-*_+ \u2603')", VARCHAR, "%7E%40%3A.-*_%2B+%E2%98%83");
        assertFunction("url_encode('test')", VARCHAR, "test");
    }

    @Test
    public void testUrlDecode()
    {
        assertFunction("url_decode('http%3A%2F%2Ftest')", VARCHAR, "http://test");
        assertFunction("url_decode('http%3A%2F%2Ftest%3Fa%3Db%26c%3Dd')", VARCHAR, "http://test?a=b&c=d");
        assertFunction("url_decode('http%3A%2F%2F%E3%83%86%E3%82%B9%E3%83%88')", VARCHAR, "http://\u30c6\u30b9\u30c8");
        assertFunction("url_decode('%7E%40%3A.-*_%2B+%E2%98%83')", VARCHAR, "~@:.-*_+ \u2603");
        assertFunction("url_decode('test')", VARCHAR, "test");

        assertInvalidFunction("url_decode('abc%x')", "URLDecoder: Incomplete trailing escape (%) pattern");
        assertInvalidFunction("url_decode('abc%fqxyz')", "URLDecoder: Illegal hex characters in escape (%) pattern - For input string: \"fq\"");
    }

    private void validateUrlExtract(String url, String protocol, String host, Long port, String path, String query, String fragment)
    {
        assertFunction("url_extract_protocol('" + url + "')", VARCHAR, protocol);
        assertFunction("url_extract_host('" + url + "')", VARCHAR, host);
        if (port == null) {
            assertFunction("url_extract_port('" + url + "')", BIGINT, null);
        }
        else {
            assertFunction("url_extract_port('" + url + "')", BIGINT, port);
        }
        assertFunction("url_extract_path('" + url + "')", VARCHAR, path);
        assertFunction("url_extract_query('" + url + "')", VARCHAR, query);
        assertFunction("url_extract_fragment('" + url + "')", VARCHAR, fragment);
    }
}
