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
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static java.util.Arrays.asList;

public class TestArraySortByKeyFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testBasic()
    {
        // Test array_sort
        assertFunction(
                "array_sort(ARRAY['pear', 'apple', 'banana', 'kiwi'], x -> length(x))",
                new ArrayType(createVarcharType(6)),
                asList("pear", "kiwi", "apple", "banana"));

        assertFunction(
                "array_sort(ARRAY['pear', 'apple', 'banana', 'kiwi'], x -> substr(x, length(x), 1))",
                new ArrayType(createVarcharType(6)),
                asList("banana", "apple", "kiwi", "pear"));

        // Test array_sort_desc
        assertFunction(
                "array_sort_desc(ARRAY['pear', 'apple', 'banana', 'kiwi'], x -> length(x))",
                new ArrayType(createVarcharType(6)),
                asList("banana", "apple", "pear", "kiwi"));

        assertFunction(
                "array_sort_desc(ARRAY['pear', 'apple', 'banana', 'kiwi'], x -> substr(x, length(x), 1))",
                new ArrayType(createVarcharType(6)),
                asList("pear", "kiwi", "apple", "banana"));
    }

    @Test
    public void testNulls()
    {
        // Test array_sort
        assertFunction(
                "array_sort(ARRAY['apple', NULL, 'banana', NULL], x -> length(x))",
                new ArrayType(createVarcharType(6)),
                asList("apple", "banana", null, null));

        assertFunction(
                "array_sort(ARRAY['apple', 'banana', 'pear'], x -> IF(x = 'banana', NULL, length(x)))",
                new ArrayType(createVarcharType(6)),
                asList("pear", "apple", "banana"));

        assertFunction(
                "array_sort(ARRAY['apple', NULL, 'banana', 'pear', NULL], x -> length(x))",
                new ArrayType(createVarcharType(6)),
                asList("pear", "apple", "banana", null, null));

        // Test array_sort_desc
        assertFunction(
                "array_sort_desc(ARRAY['apple', NULL, 'banana', NULL], x -> length(x))",
                new ArrayType(createVarcharType(6)),
                asList("banana", "apple", null, null));

        assertFunction(
                "array_sort_desc(ARRAY['apple', 'banana', 'pear'], x -> IF(x = 'banana', NULL, length(x)))",
                new ArrayType(createVarcharType(6)),
                asList("apple", "pear", "banana"));

        assertFunction(
                "array_sort_desc(ARRAY['apple', NULL, 'banana', 'pear', NULL], x -> length(x))",
                new ArrayType(createVarcharType(6)),
                asList("banana", "apple", "pear", null, null));
    }

    @Test
    public void testSpecialDoubleValues()
    {
        // Test array_sort
        assertFunction(
                "array_sort(ARRAY[CAST(0.0 AS DOUBLE), CAST('NaN' AS DOUBLE), CAST('Infinity' AS DOUBLE), CAST('-Infinity' AS DOUBLE)], x -> x)",
                new ArrayType(DOUBLE),
                asList(Double.NEGATIVE_INFINITY, 0.0, Double.POSITIVE_INFINITY, Double.NaN));

        // Test array_sort_desc
        assertFunction(
                "array_sort_desc(ARRAY[CAST(0.0 AS DOUBLE), CAST('NaN' AS DOUBLE), CAST('Infinity' AS DOUBLE), CAST('-Infinity' AS DOUBLE)], x -> x)",
                new ArrayType(DOUBLE),
                asList(Double.NaN, Double.POSITIVE_INFINITY, 0.0, Double.NEGATIVE_INFINITY));
    }

    @Test
    public void testNumericKeys()
    {
        // Test array_sort
        assertFunction(
                "array_sort(ARRAY[5, 20, 3, 9, 100], x -> x)",
                new ArrayType(INTEGER),
                asList(3, 5, 9, 20, 100));

        assertFunction(
                "array_sort(ARRAY[CAST(5000000000 AS BIGINT), CAST(20000000000 AS BIGINT), CAST(3000000000 AS BIGINT), CAST(9000000000 AS BIGINT), CAST(100000000000 AS BIGINT)], x -> x)",
                new ArrayType(BIGINT),
                asList(3000000000L, 5000000000L, 9000000000L, 20000000000L, 100000000000L));

        assertFunction(
                "array_sort(ARRAY[CAST(5.5 AS DOUBLE), CAST(20.1 AS DOUBLE), CAST(3.9 AS DOUBLE), CAST(9.0 AS DOUBLE), CAST(100.0 AS DOUBLE)], x -> x)",
                new ArrayType(DOUBLE),
                asList(3.9, 5.5, 9.0, 20.1, 100.0));

        assertFunction(
                "array_sort(ARRAY[5, 20, 3, 9, 100], x -> x % 10)",
                new ArrayType(INTEGER),
                asList(20, 100, 3, 5, 9));

        assertFunction(
                "array_sort(ARRAY[CAST(5000000000 AS BIGINT), CAST(20000000000 AS BIGINT), CAST(3000000000 AS BIGINT), CAST(9000000000 AS BIGINT), CAST(100000000000 AS BIGINT)], x -> x % CAST(10000000000 AS BIGINT))",
                new ArrayType(BIGINT),
                asList(20000000000L, 100000000000L, 3000000000L, 5000000000L, 9000000000L));

        // Test array_sort_desc
        assertFunction(
                "array_sort_desc(ARRAY[5, 20, 3, 9, 100], x -> x)",
                new ArrayType(INTEGER),
                asList(100, 20, 9, 5, 3));

        assertFunction(
                "array_sort_desc(ARRAY[CAST(5000000000 AS BIGINT), CAST(20000000000 AS BIGINT), CAST(3000000000 AS BIGINT), CAST(9000000000 AS BIGINT), CAST(100000000000 AS BIGINT)], x -> x)",
                new ArrayType(BIGINT),
                asList(100000000000L, 20000000000L, 9000000000L, 5000000000L, 3000000000L));

        assertFunction(
                "array_sort_desc(ARRAY[CAST(5.5 AS DOUBLE), CAST(20.1 AS DOUBLE), CAST(3.9 AS DOUBLE), CAST(9.0 AS DOUBLE), CAST(100.0 AS DOUBLE)], x -> x)",
                new ArrayType(DOUBLE),
                asList(100.0, 20.1, 9.0, 5.5, 3.9));

        assertFunction(
                "array_sort_desc(ARRAY[5, 20, 3, 9, 100], x -> x % 10)",
                new ArrayType(INTEGER),
                asList(9, 5, 3, 20, 100));

        assertFunction(
                "array_sort_desc(ARRAY[CAST(5000000000 AS BIGINT), CAST(20000000000 AS BIGINT), CAST(3000000000 AS BIGINT), CAST(9000000000 AS BIGINT), CAST(100000000000 AS BIGINT)], x -> x % CAST(10000000000 AS BIGINT))",
                new ArrayType(BIGINT),
                asList(9000000000L, 5000000000L, 3000000000L, 20000000000L, 100000000000L));
    }

    @Test
    public void testBooleanKeys()
    {
        // Test array_sort
        assertFunction(
                "array_sort(ARRAY[true, false, true, false], x -> x)",
                new ArrayType(BOOLEAN),
                asList(false, false, true, true));

        assertFunction(
                "array_sort(ARRAY[true, false, true, false], x -> NOT x)",
                new ArrayType(BOOLEAN),
                asList(true, true, false, false));

        // Test array_sort_desc
        assertFunction(
                "array_sort_desc(ARRAY[true, false, true, false], x -> x)",
                new ArrayType(BOOLEAN),
                asList(true, true, false, false));

        assertFunction(
                "array_sort_desc(ARRAY[true, false, true, false], x -> NOT x)",
                new ArrayType(BOOLEAN),
                asList(false, false, true, true));
    }

    @Test
    public void testComplexTypes()
    {
        // Test array_sort
        assertFunction(
                "array_sort(ARRAY[ARRAY[1, 2, 3], ARRAY[4, 5], ARRAY[6, 7, 8, 9]], x -> cardinality(x))",
                new ArrayType(new ArrayType(INTEGER)),
                asList(asList(4, 5), asList(1, 2, 3), asList(6, 7, 8, 9)));

        assertFunction(
                "array_sort(ARRAY[ROW('a', 3), ROW('b', 1), ROW('c', 2)], x -> x[2])",
                new ArrayType(RowType.anonymous(ImmutableList.of(createVarcharType(1), INTEGER))),
                asList(asList("b", 1), asList("c", 2), asList("a", 3)));

        assertFunction(
                "array_sort(ARRAY[ROW('a', CAST(3000000000 AS BIGINT)), ROW('b', CAST(1000000000 AS BIGINT)), ROW('c', CAST(2000000000 AS BIGINT))], x -> x[2])",
                new ArrayType(RowType.anonymous(ImmutableList.of(createVarcharType(1), BIGINT))),
                asList(asList("b", 1000000000L), asList("c", 2000000000L), asList("a", 3000000000L)));

        // Test array_sort_desc
        assertFunction(
                "array_sort_desc(ARRAY[ARRAY[1, 2, 3], ARRAY[4, 5], ARRAY[6, 7, 8, 9]], x -> cardinality(x))",
                new ArrayType(new ArrayType(INTEGER)),
                asList(asList(6, 7, 8, 9), asList(1, 2, 3), asList(4, 5)));

        assertFunction(
                "array_sort_desc(ARRAY[ROW('a', 3), ROW('b', 1), ROW('c', 2)], x -> x[2])",
                new ArrayType(RowType.anonymous(ImmutableList.of(createVarcharType(1), INTEGER))),
                asList(asList("a", 3), asList("c", 2), asList("b", 1)));

        assertFunction(
                "array_sort_desc(ARRAY[ROW('a', CAST(3000000000 AS BIGINT)), ROW('b', CAST(1000000000 AS BIGINT)), ROW('c', CAST(2000000000 AS BIGINT))], x -> x[2])",
                new ArrayType(RowType.anonymous(ImmutableList.of(createVarcharType(1), BIGINT))),
                asList(asList("a", 3000000000L), asList("c", 2000000000L), asList("b", 1000000000L)));
    }

    @Test
    public void testEdgeCases()
    {
        // Test array_sort
        assertFunction(
                "array_sort(ARRAY[], x -> x)",
                new ArrayType(UNKNOWN),
                ImmutableList.of());

        assertFunction(
                "array_sort(ARRAY[5], x -> x)",
                new ArrayType(INTEGER),
                asList(5));

        assertFunction(
                "array_sort(ARRAY[NULL, NULL, NULL], x -> x)",
                new ArrayType(UNKNOWN),
                asList(null, null, null));

        // Test array_sort_desc
        assertFunction(
                "array_sort_desc(ARRAY[], x -> x)",
                new ArrayType(UNKNOWN),
                ImmutableList.of());

        assertFunction(
                "array_sort_desc(ARRAY[5], x -> x)",
                new ArrayType(INTEGER),
                asList(5));

        assertFunction(
                "array_sort_desc(ARRAY[NULL, NULL, NULL], x -> x)",
                new ArrayType(UNKNOWN),
                asList(null, null, null));
    }

    @Test
    public void testTypeCoercion()
    {
        // Test array_sort
        assertFunction(
                "array_sort(ARRAY[5, 20, 3, 9, 100], x -> x + CAST(0.5 AS DOUBLE))",
                new ArrayType(INTEGER),
                asList(3, 5, 9, 20, 100));

        assertFunction(
                "array_sort(ARRAY[5, 20, 3, 9, 100], x -> x * CAST(1000000000 AS BIGINT))",
                new ArrayType(INTEGER),
                asList(3, 5, 9, 20, 100));

        assertFunction(
                "array_sort(ARRAY['5', '20', '3', '9', '100'], x -> cast(x as integer))",
                new ArrayType(createVarcharType(3)),
                asList("3", "5", "9", "20", "100"));

        assertFunction(
                "array_sort(ARRAY['5000000000', '20000000000', '3000000000', '9000000000', '100000000000'], x -> cast(x as bigint))",
                new ArrayType(createVarcharType(12)),
                asList("3000000000", "5000000000", "9000000000", "20000000000", "100000000000"));

        // Test array_sort_desc
        assertFunction(
                "array_sort_desc(ARRAY[5, 20, 3, 9, 100], x -> x + CAST(0.5 AS DOUBLE))",
                new ArrayType(INTEGER),
                asList(100, 20, 9, 5, 3));

        assertFunction(
                "array_sort_desc(ARRAY[5, 20, 3, 9, 100], x -> x * CAST(1000000000 AS BIGINT))",
                new ArrayType(INTEGER),
                asList(100, 20, 9, 5, 3));

        assertFunction(
                "array_sort_desc(ARRAY['5', '20', '3', '9', '100'], x -> cast(x as integer))",
                new ArrayType(createVarcharType(3)),
                asList("100", "20", "9", "5", "3"));

        assertFunction(
                "array_sort_desc(ARRAY['5000000000', '20000000000', '3000000000', '9000000000', '100000000000'], x -> cast(x as bigint))",
                new ArrayType(createVarcharType(12)),
                asList("100000000000", "20000000000", "9000000000", "5000000000", "3000000000"));
    }
}
