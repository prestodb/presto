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
package com.facebook.presto.operator.scalar.sql;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;

public class TestSplitPartReverseFunction
        extends AbstractTestFunctions
{
    @Test
    public void testPositiveIndex()
    {
        assertFunction(
                "SPLIT_PART_REVERSE('foo/bar/baz/qux', '/', 1)",
                VARCHAR,
                "foo");
        assertFunction(
                "SPLIT_PART_REVERSE('foo/bar/baz/qux', '/', 2)",
                VARCHAR,
                "bar");
        assertFunction(
                "SPLIT_PART_REVERSE('foo/bar/baz/qux', '/', 4)",
                VARCHAR,
                "qux");
    }

    @Test
    public void testNegativeIndex()
    {
        assertFunction(
                "SPLIT_PART_REVERSE('foo/bar/baz/qux', '/', -1)",
                VARCHAR,
                "qux");
        assertFunction(
                "SPLIT_PART_REVERSE('foo/bar/baz/qux', '/', -2)",
                VARCHAR,
                "baz");
        assertFunction(
                "SPLIT_PART_REVERSE('foo/bar/baz/qux', '/', -4)",
                VARCHAR,
                "foo");
    }

    @Test
    public void testOutOfBounds()
    {
        assertFunction(
                "SPLIT_PART_REVERSE('foo/bar/baz', '/', 5)",
                VARCHAR,
                null);
        assertFunction(
                "SPLIT_PART_REVERSE('foo/bar/baz', '/', -5)",
                VARCHAR,
                null);
    }

    @Test
    public void testCommaDelimiter()
    {
        assertFunction(
                "SPLIT_PART_REVERSE('I,he,she,they', ',', -1)",
                VARCHAR,
                "they");
        assertFunction(
                "SPLIT_PART_REVERSE('I,he,she,they', ',', 1)",
                VARCHAR,
                "I");
    }

    @Test
    public void testEmptyParts()
    {
        assertFunction(
                "SPLIT_PART_REVERSE('one,,,four,', ',', -1)",
                VARCHAR,
                "");
        assertFunction(
                "SPLIT_PART_REVERSE('one,,,four,', ',', -2)",
                VARCHAR,
                "four");
        assertFunction(
                "SPLIT_PART_REVERSE('one,,,four,', ',', 1)",
                VARCHAR,
                "one");
    }

    @Test
    public void testNoDelimiter()
    {
        assertFunction(
                "SPLIT_PART_REVERSE('abc', ',', 1)",
                VARCHAR,
                "abc");
        assertFunction(
                "SPLIT_PART_REVERSE('abc', ',', -1)",
                VARCHAR,
                "abc");
        assertFunction(
                "SPLIT_PART_REVERSE('abc', ',', 2)",
                VARCHAR,
                null);
    }

    @Test
    public void testEmptyString()
    {
        assertFunction(
                "SPLIT_PART_REVERSE('', ',', 1)",
                VARCHAR,
                "");
        assertFunction(
                "SPLIT_PART_REVERSE('', ',', -1)",
                VARCHAR,
                "");
    }

    @Test
    public void testNull()
    {
        assertFunction(
                "SPLIT_PART_REVERSE(CAST(NULL AS VARCHAR), '/', 1)",
                VARCHAR,
                null);
        assertFunction(
                "SPLIT_PART_REVERSE('foo/bar', NULL, 1)",
                VARCHAR,
                null);
    }

    @Test
    public void testError()
    {
        assertInvalidFunction(
                "SPLIT_PART_REVERSE('foo/bar', '/', 'one')",
                SemanticErrorCode.FUNCTION_NOT_FOUND);
        assertInvalidFunction(
                "SPLIT_PART_REVERSE(123, '/', 1)",
                SemanticErrorCode.FUNCTION_NOT_FOUND);
    }
}
