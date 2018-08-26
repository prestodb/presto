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

import com.facebook.presto.spi.type.ArrayType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.VarcharType.createVarcharType;

public class TestNgramsFunction
        extends AbstractTestFunctions
{
    @Test
    public void testNgrams()
    {
        assertFunction("ngrams('foo bar baz', 2, ' ', ',')", new ArrayType(createVarcharType(11)), ImmutableList.of("foo,bar", "bar,baz"));
        assertFunction("ngrams('foo bar baz', 2, ' ')", new ArrayType(createVarcharType(11)), ImmutableList.of("foo bar", "bar baz"));
        assertFunction("ngrams('foo bar baz', 3, ' ')", new ArrayType(createVarcharType(11)), ImmutableList.of("foo bar baz"));
        assertFunction("ngrams('foo bar baz', 4, ' ')", new ArrayType(createVarcharType(11)), ImmutableList.of("foo bar baz"));
        assertFunction("ngrams('foo bar baz', 2, ' ')", new ArrayType(createVarcharType(11)), ImmutableList.of("foo bar", "bar baz"));
        assertFunction("ngrams('foo bar baz', 1, ' ')", new ArrayType(createVarcharType(11)), ImmutableList.of("foo", "bar", "baz"));
        assertFunction("ngrams('foo bar baz ', 1, ' ')", new ArrayType(createVarcharType(12)), ImmutableList.of("foo", "bar", "baz", ""));

        // Test split for non-ASCII
        assertFunction("ngrams('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', 2, ',', '')", new ArrayType(createVarcharType(7)), ImmutableList.of("\u4FE1\u5FF5\u7231", "\u7231\u5E0C\u671B"));
        assertFunction("ngrams('\u8B49\u8BC1\u8A3C\u8BC1\u7231', 2, '\u8BC1', '')", new ArrayType(createVarcharType(5)), ImmutableList.of("\u8B49\u8A3C", "\u8A3C\u7231"));

        // Test delimiter is empty
        assertFunction("ngrams('foo', 1, '', '')", new ArrayType(createVarcharType(3)), ImmutableList.of("f", "o", "o"));
        assertFunction("ngrams('foobar', 4, '', '')", new ArrayType(createVarcharType(6)), ImmutableList.of("foob", "ooba", "obar"));

        assertInvalidFunction("ngrams('foo', 0)", "N must be positive");
        assertInvalidFunction("ngrams('foo', -1)", "N must be positive");
        assertInvalidFunction("ngrams('foo', 2147483648)", "N is too large");
    }
}
