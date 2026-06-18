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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static java.util.Arrays.asList;

public class TestArrayUnionFunction
        extends AbstractTestFunctions
{
    @Test
    public void testBigint()
    {
        assertFunction("ARRAY_UNION(ARRAY [cast(10 as bigint), NULL, cast(12 as bigint), NULL], ARRAY [NULL, cast(10 as bigint), NULL, NULL])", new ArrayType(BIGINT), asList(10L, null, 12L));
    }

    public void testInteger()
    {
        assertFunction("ARRAY_UNION(ARRAY [12], ARRAY [10])", new ArrayType(INTEGER), ImmutableList.of(12, 10));
        assertFunction("ARRAY_UNION(ARRAY [1, 5], ARRAY [1])", new ArrayType(INTEGER), ImmutableList.of(1, 5));
        assertFunction("ARRAY_UNION(ARRAY [1, 1, 2, 4], ARRAY [1, 1, 4, 4])", new ArrayType(INTEGER), ImmutableList.of(1, 2, 4));
        assertFunction("ARRAY_UNION(ARRAY [2, 8], ARRAY [8, 3])", new ArrayType(INTEGER), ImmutableList.of(2, 8, 3));
        assertFunction("ARRAY_UNION(ARRAY [IF (RAND() < 1.0E0, 7, 1) , 2], ARRAY [7])", new ArrayType(INTEGER), ImmutableList.of(7, 2));
    }

    @Test
    public void testVarchar()
    {
        assertFunction("ARRAY_UNION(ARRAY ['foo', 'bar', 'baz'], ARRAY ['foo', 'test', 'bar'])", new ArrayType(createVarcharType(4)), ImmutableList.of("foo", "bar", "baz", "test"));
    }

    @Test
    public void testDouble()
    {
        assertFunction("ARRAY_UNION(ARRAY [1, 5], ARRAY [1.0E0])", new ArrayType(DOUBLE), ImmutableList.of(1.0, 5.0));
        assertFunction("ARRAY_UNION(ARRAY [8.3E0, 1.6E0, 4.1E0, 5.2E0], ARRAY [4.0E0, 5.2E0, 8.3E0, 9.7E0, 3.5E0])", new ArrayType(DOUBLE), ImmutableList.of(8.3, 1.6, 4.1, 5.2, 4.0, 9.7, 3.5));
        assertFunction("ARRAY_UNION(ARRAY [5.1E0, 7, 3.0E0, 4.8E0, 10], ARRAY [6.5E0, 10.0E0, 1.9E0, 5.1E0, 3.9E0, 4.8E0])", new ArrayType(DOUBLE), ImmutableList.of(5.1, 7.0, 3.0, 4.8, 10.0, 6.5, 1.9, 3.9));
    }

    @Test
    public void testArrayOfArrays()
    {
        assertFunction("ARRAY_UNION(ARRAY [ARRAY [4, 5], ARRAY [6, 7]], ARRAY [ARRAY [4, 5], ARRAY [6, 8]])", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(4, 5), ImmutableList.of(6, 7), ImmutableList.of(6, 8)));
    }

    @Test
    public void testNull()
    {
        assertFunction("ARRAY_UNION(ARRAY [NULL], ARRAY [NULL, NULL])", new ArrayType(UNKNOWN), asList((Object) null));
        assertFunction("ARRAY_UNION(ARRAY ['abc', NULL, 'xyz', NULL], ARRAY [NULL, 'abc', NULL, NULL])", new ArrayType(createVarcharType(3)), asList("abc", null, "xyz"));
    }

    @Test
    public void testIndeterminateRows()
    {
        // test unsupported
        assertFunction(
                "array_union(ARRAY[(123, 'abc'), (123, NULL)], ARRAY[(123, 'abc'), (123, NULL)])",
                new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, createVarcharType(3)))),
                ImmutableList.of(asList(123, "abc"), asList(123, null)));
        assertFunction(
                "array_union(ARRAY[(NULL, 'abc'), (123, null), (123, 'abc')], ARRAY[(456, 'def'),(NULL, 'abc')])",
                new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, createVarcharType(3)))),
                ImmutableList.of(asList(null, "abc"), asList(123, null), asList(123, "abc"), asList(456, "def")));
    }

    @Test
    public void testIndeterminateArrays()
    {
        assertFunction(
                "array_union(ARRAY[ARRAY[123, 456], ARRAY[123, NULL]], ARRAY[ARRAY[123, 456], ARRAY[123, NULL]])",
                new ArrayType(new ArrayType(INTEGER)),
                ImmutableList.of(asList(123, 456), asList(123, null)));
        assertFunction(
                "array_union(ARRAY[ARRAY[NULL, 456], ARRAY[123, 456]], ARRAY[ARRAY[123, 456],ARRAY[NULL, 456]])",
                new ArrayType(new ArrayType(INTEGER)),
                ImmutableList.of(asList(null, 456), asList(123, 456)));
    }
}
