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
package io.prestosql.operator.scalar;

import org.testng.annotations.Test;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.createVarcharType;

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
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k1')", createVarcharType(53), "v1");
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k2')", createVarcharType(53), "v2");
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k3')", createVarcharType(53), "");
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k4')", createVarcharType(53), "");
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k5')", createVarcharType(53), null);
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k1=v2&k1&k1#Ref1', 'k1')", createVarcharType(53), "v1");
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k1&k1=v1&k1&k1#Ref1', 'k1')", createVarcharType(50), "");
        assertFunction("url_extract_parameter('http://example.com/path1/p.php?k=a=b=c&x=y#Ref1', 'k')", createVarcharType(47), "a=b=c");
        assertFunction("url_extract_parameter('foo', 'k1')", createVarcharType(3), null);
    }

    @Test
    public void testUrlEncode()
    {
        final String[][] outputInputPairs = {
                {"http%3A%2F%2Ftest", "http://test"},
                {"http%3A%2F%2Ftest%3Fa%3Db%26c%3Dd", "http://test?a=b&c=d"},
                {"http%3A%2F%2F%E3%83%86%E3%82%B9%E3%83%88", "http://\u30c6\u30b9\u30c8"},
                {"%7E%40%3A.-*_%2B+%E2%98%83", "~@:.-*_+ \u2603"},
                {"test", "test"},
        };

        for (String[] outputInputPair : outputInputPairs) {
            String input = outputInputPair[1];
            String output = outputInputPair[0];
            assertFunction("url_encode('" + input + "')", createVarcharType(input.length() * 12), output);
        }

        assertFunction("url_encode('\uD867\uDE3D')", createVarcharType(12), "%F0%A9%B8%BD");
    }

    @Test
    public void testUrlDecode()
    {
        String[][] inputOutputPairs = {
                {"http%3A%2F%2Ftest", "http://test"},
                {"http%3A%2F%2Ftest%3Fa%3Db%26c%3Dd", "http://test?a=b&c=d"},
                {"http%3A%2F%2F%E3%83%86%E3%82%B9%E3%83%88", "http://\u30c6\u30b9\u30c8"},
                {"%7E%40%3A.-*_%2B+%E2%98%83", "~@:.-*_+ \u2603"},
                {"test", "test"},
        };

        for (String[] inputOutputPair : inputOutputPairs) {
            String input = inputOutputPair[0];
            String output = inputOutputPair[1];
            assertFunction("url_decode('" + input + "')", createVarcharType(input.length()), output);
        }
    }

    private void validateUrlExtract(String url, String protocol, String host, Long port, String path, String query, String fragment)
    {
        assertFunction("url_extract_protocol('" + url + "')", createVarcharType(url.length()), protocol);
        assertFunction("url_extract_host('" + url + "')", createVarcharType(url.length()), host);
        if (port == null) {
            assertFunction("url_extract_port('" + url + "')", BIGINT, null);
        }
        else {
            assertFunction("url_extract_port('" + url + "')", BIGINT, port);
        }
        assertFunction("url_extract_path('" + url + "')", createVarcharType(url.length()), path);
        assertFunction("url_extract_query('" + url + "')", createVarcharType(url.length()), query);
        assertFunction("url_extract_fragment('" + url + "')", createVarcharType(url.length()), fragment);
    }
}
