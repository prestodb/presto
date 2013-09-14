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

import static com.facebook.presto.operator.scalar.FunctionAssertions.assertFunction;
import static com.facebook.presto.operator.scalar.FunctionAssertions.assertFunctionNull;

public class TestUrlFunctions
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
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k1')", "v1");
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k2')", "v2");
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k3')", "");
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k4')", "");
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k5')", null);
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k1=v2&k1&k1#Ref1', 'k1')", "v1");
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k1&k1=v1&k1&k1#Ref1', 'k1')", "");
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k=a=b=c&x=y#Ref1', 'k')", "a=b=c");
        assertFunction("url_extract_parameter('foo', 'k1')", null);
    }

    private static void validateUrlExtract(String url, String protocol, String host, Long port, String path, String query, String fragment)
    {
        assertFunction("url_extract_protocol('" + url + "')", protocol);
        assertFunction("url_extract_host('" + url + "')", host);
        if (port == null) {
            assertFunctionNull("url_extract_port('" + url + "')");
        }
        else {
            assertFunction("url_extract_port('" + url + "')", port);
        }
        assertFunction("url_extract_path('" + url + "')", path);
        assertFunction("url_extract_query('" + url + "')", query);
        assertFunction("url_extract_fragment('" + url + "')", fragment);
    }
}
