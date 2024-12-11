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
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class TestArrayIntersectFunction
        extends AbstractTestFunctions
{
    @Test
    public void testVarchar()
    {
        assertFunction("ARRAY_INTERSECT(ARRAY[CAST('x' as VARCHAR), 'y', 'z'], ARRAY['x', 'y'])", new ArrayType(VARCHAR), ImmutableList.of("x", "y"));
        assertFunction("ARRAY_INTERSECT(ARRAY ['abc'], ARRAY ['abc', 'bcd'])", new ArrayType(createVarcharType(3)), ImmutableList.of("abc"));
        assertFunction("ARRAY_INTERSECT(ARRAY ['abc', 'abc'], ARRAY ['abc', 'abc'])", new ArrayType(createVarcharType(3)), ImmutableList.of("abc"));
        assertFunction("ARRAY_INTERSECT(ARRAY ['foo', 'bar', 'baz'], ARRAY ['foo', 'test', 'bar'])", new ArrayType(createVarcharType(4)), ImmutableList.of("foo", "bar"));
    }

    @Test
    public void testBigint()
    {
        assertFunction("ARRAY_INTERSECT(ARRAY[CAST(1 as BIGINT), 5, 3], ARRAY[5])", new ArrayType(BIGINT), ImmutableList.of(5L));
        assertFunction("ARRAY_INTERSECT(ARRAY [CAST(5 AS BIGINT), CAST(5 AS BIGINT)], ARRAY [CAST(1 AS BIGINT), CAST(5 AS BIGINT)])", new ArrayType(BIGINT), ImmutableList.of(5L));
    }

    @Test
    public void testInteger()
    {
        assertFunction("ARRAY_INTERSECT(ARRAY[1, 5, 3], ARRAY[3])", new ArrayType(INTEGER), ImmutableList.of(3));
        assertFunction("ARRAY_INTERSECT(ARRAY [5], ARRAY [5])", new ArrayType(INTEGER), ImmutableList.of(5));
        assertFunction("ARRAY_INTERSECT(ARRAY [1, 2, 5, 5, 6], ARRAY [5, 5, 6, 6, 7, 8])", new ArrayType(INTEGER), ImmutableList.of(5, 6));
        assertFunction("ARRAY_INTERSECT(ARRAY [IF (RAND() < 1.0E0, 7, 1) , 2], ARRAY [7])", new ArrayType(INTEGER), ImmutableList.of(7));
    }

    @Test
    public void testDouble()
    {
        assertFunction("ARRAY_INTERSECT(ARRAY [1, 5], ARRAY [1.0E0])", new ArrayType(DOUBLE), ImmutableList.of(1.0));
        assertFunction("ARRAY_INTERSECT(ARRAY [1.0E0, 5.0E0], ARRAY [5.0E0, 5.0E0, 6.0E0])", new ArrayType(DOUBLE), ImmutableList.of(5.0));
        assertFunction("ARRAY_INTERSECT(ARRAY [8.3E0, 1.6E0, 4.1E0, 5.2E0], ARRAY [4.0E0, 5.2E0, 8.3E0, 9.7E0, 3.5E0])", new ArrayType(DOUBLE), ImmutableList.of(5.2, 8.3));
        assertFunction("ARRAY_INTERSECT(ARRAY [5.1E0, 7, 3.0E0, 4.8E0, 10], ARRAY [6.5E0, 10.0E0, 1.9E0, 5.1E0, 3.9E0, 4.8E0])", new ArrayType(DOUBLE), ImmutableList.of(10.0, 5.1, 4.8));
        assertFunction("ARRAY_INTERSECT(ARRAY[1.1E0, 5.4E0, 3.9E0], ARRAY[5, 5.4E0])", new ArrayType(DOUBLE), ImmutableList.of(5.4));
    }

    @Test
    public void testDecimal()
    {
        assertFunction(
                "ARRAY_INTERSECT(ARRAY [2.3, 2.3, 2.2], ARRAY[2.2, 2.3])",
                new ArrayType(createDecimalType(2, 1)),
                ImmutableList.of(decimal("2.3"), decimal("2.2")));
        assertFunction("ARRAY_INTERSECT(ARRAY [2.330, 1.900, 2.330], ARRAY [2.3300, 1.9000])", new ArrayType(createDecimalType(5, 4)),
                ImmutableList.of(decimal("2.3300"), decimal("1.9000")));
        assertFunction("ARRAY_INTERSECT(ARRAY [2, 3], ARRAY[2.0, 3.0])", new ArrayType(createDecimalType(11, 1)),
                ImmutableList.of(decimal("00000000002.0"), decimal("00000000003.0")));
    }

    @Test
    public void testBoolean()
    {
        assertFunction("ARRAY_INTERSECT(ARRAY [true], ARRAY [true])", new ArrayType(BOOLEAN), ImmutableList.of(true));
        assertFunction("ARRAY_INTERSECT(ARRAY [true, false], ARRAY [true])", new ArrayType(BOOLEAN), ImmutableList.of(true));
        assertFunction("ARRAY_INTERSECT(ARRAY [true, true], ARRAY [true, true])", new ArrayType(BOOLEAN), ImmutableList.of(true));
        assertFunction("ARRAY_INTERSECT(ARRAY[true, false, null], ARRAY[true, null])", new ArrayType(BOOLEAN), asList(true, null));
    }

    @Test
    public void testRow()
    {
        assertFunction(
                "ARRAY_INTERSECT(ARRAY[(123, 456), (123, 789)], ARRAY[(123, 456), (123, 456), (123, 789)])",
                new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, INTEGER))),
                ImmutableList.of(asList(123, 456), asList(123, 789)));
        assertFunction(
                "ARRAY_INTERSECT(ARRAY[ARRAY[123, 456], ARRAY[123, 789]], ARRAY[ARRAY[123, 456], ARRAY[123, 456], ARRAY[123, 789]])",
                new ArrayType(new ArrayType((INTEGER))),
                ImmutableList.of(asList(123, 456), asList(123, 789)));
        assertFunction(
                "ARRAY_INTERSECT(ARRAY[(123, 'abc'), (123, 'cde')], ARRAY[(123, 'abc'), (123, 'cde')])",
                new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, createVarcharType(3)))),
                ImmutableList.of(asList(123, "abc"), asList(123, "cde")));
        assertFunction(
                "ARRAY_INTERSECT(ARRAY[(123, 'abc'), (123, 'cde'), NULL], ARRAY[(123, 'abc'), (123, 'cde')])",
                new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, createVarcharType(3)))),
                ImmutableList.of(asList(123, "abc"), asList(123, "cde")));
        assertFunction(
                "ARRAY_INTERSECT(ARRAY[(123, 'abc'), (123, 'cde'), NULL, NULL], ARRAY[(123, 'abc'), (123, 'cde'), NULL])",
                new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, createVarcharType(3)))),
                asList(asList(123, "abc"), asList(123, "cde"), null));
        assertFunction(
                "ARRAY_INTERSECT(ARRAY[(123, 'abc'), (123, 'abc')], ARRAY[(123, 'abc'), (123, NULL)])",
                new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, createVarcharType(3)))),
                ImmutableList.of(asList(123, "abc")));
        assertFunction(
                "ARRAY_INTERSECT(ARRAY[(123, 'abc')], ARRAY[(123, NULL)])",
                new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, createVarcharType(3)))),
                ImmutableList.of());
    }

    @Test
    public void testIndeterminateRows()
    {
        assertFunction(
                "ARRAY_INTERSECT(ARRAY[(123, 'abc'), (123, NULL)], ARRAY[(123, 'abc'), (123, NULL)])",
                new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, createVarcharType(3)))),
                ImmutableList.of(asList(123, "abc"), asList(123, null)));
        assertFunction(
                "ARRAY_INTERSECT(ARRAY[(NULL, 'abc'), (123, 'abc')], ARRAY[(123, 'abc'),(NULL, 'abc')])",
                new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, createVarcharType(3)))),
                ImmutableList.of(asList(null, "abc"), asList(123, "abc")));
    }

    @Test
    public void testIndeterminateArrays()
    {
        assertFunction(
                "ARRAY_INTERSECT(ARRAY[ARRAY[123, 456], ARRAY[123, NULL]], ARRAY[ARRAY[123, 456], ARRAY[123, NULL]])",
                new ArrayType(new ArrayType(INTEGER)),
                ImmutableList.of(asList(123, 456), asList(123, null)));
        assertFunction(
                "ARRAY_INTERSECT(ARRAY[ARRAY[NULL, 456], ARRAY[123, 456]], ARRAY[ARRAY[123, 456],ARRAY[NULL, 456]])",
                new ArrayType(new ArrayType(INTEGER)),
                ImmutableList.of(asList(null, 456), asList(123, 456)));
    }

    @Test
    public void testUnboundedRetainedSize()
    {
        assertCachedInstanceHasBoundedRetainedSize("ARRAY_INTERSECT(ARRAY ['foo', 'bar', 'baz'], ARRAY ['foo', 'test', 'bar'])");
    }

    @Test
    public void testEmptyArrays()
    {
        assertFunction("ARRAY_INTERSECT(ARRAY[], ARRAY[])", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("ARRAY_INTERSECT(ARRAY[], ARRAY[1, 3])", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("ARRAY_INTERSECT(ARRAY [], ARRAY [5])", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("ARRAY_INTERSECT(ARRAY [5, 6], ARRAY [])", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("ARRAY_INTERSECT(ARRAY ['abc'], ARRAY [])", new ArrayType(createVarcharType(3)), ImmutableList.of());
        assertFunction("ARRAY_INTERSECT(ARRAY [], ARRAY ['abc', 'bcd'])", new ArrayType(createVarcharType(3)), ImmutableList.of());
        assertFunction("ARRAY_INTERSECT(ARRAY [], ARRAY [NULL])", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("ARRAY_INTERSECT(ARRAY [], ARRAY [false])", new ArrayType(BOOLEAN), ImmutableList.of());
    }

    @Test
    public void testEmptyResults()
    {
        assertFunction("ARRAY_INTERSECT(ARRAY [1], ARRAY [5])", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("ARRAY_INTERSECT(ARRAY [CAST(1 AS BIGINT)], ARRAY [CAST(5 AS BIGINT)])", new ArrayType(BIGINT), ImmutableList.of());
        assertFunction("ARRAY_INTERSECT(ARRAY [true, true], ARRAY [false])", new ArrayType(BOOLEAN), ImmutableList.of());
        assertFunction("ARRAY_INTERSECT(ARRAY [5], ARRAY [1.0E0])", new ArrayType(DOUBLE), ImmutableList.of());
    }

    @Test
    public void testNull()
    {
        assertFunction("ARRAY_INTERSECT(NULL, NULL)", new ArrayType(UNKNOWN), null);
        assertFunction("ARRAY_INTERSECT(ARRAY[NULL], NULL)", new ArrayType(UNKNOWN), null);
        assertFunction("ARRAY_INTERSECT(NULL, ARRAY[NULL])", new ArrayType(UNKNOWN), null);
        assertFunction("ARRAY_INTERSECT(ARRAY[NULL], ARRAY[NULL])", new ArrayType(UNKNOWN), asList(false ? 1 : null));
        assertFunction("ARRAY_INTERSECT(ARRAY[], ARRAY[NULL])", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("ARRAY_INTERSECT(ARRAY[NULL], ARRAY[])", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("ARRAY_INTERSECT(ARRAY [NULL], ARRAY [NULL, NULL])", new ArrayType(UNKNOWN), asList((Object) null));

        assertFunction("ARRAY_INTERSECT(ARRAY [0, 0, 1, NULL], ARRAY [0, 0, 1, NULL])", new ArrayType(INTEGER), asList(0, 1, null));
        assertFunction("ARRAY_INTERSECT(ARRAY [0, 0], ARRAY [0, 0, NULL])", new ArrayType(INTEGER), ImmutableList.of(0));
        assertFunction("ARRAY_INTERSECT(ARRAY [CAST(0 AS BIGINT), CAST(0 AS BIGINT)], ARRAY [CAST(0 AS BIGINT), NULL])", new ArrayType(BIGINT), ImmutableList.of(0L));
        assertFunction("ARRAY_INTERSECT(ARRAY [0.0E0], ARRAY [NULL])", new ArrayType(DOUBLE), ImmutableList.of());
        assertFunction("ARRAY_INTERSECT(ARRAY [0.0E0, NULL], ARRAY [0.0E0, NULL])", new ArrayType(DOUBLE), asList(0.0, null));
        assertFunction("ARRAY_INTERSECT(ARRAY [true, true, false, false, NULL], ARRAY [true, false, false, NULL])", new ArrayType(BOOLEAN), asList(true, false, null));
        assertFunction("ARRAY_INTERSECT(ARRAY [false, false], ARRAY [false, false, NULL])", new ArrayType(BOOLEAN), ImmutableList.of(false));
        assertFunction("ARRAY_INTERSECT(ARRAY ['abc'], ARRAY [NULL])", new ArrayType(createVarcharType(3)), ImmutableList.of());
        assertFunction("ARRAY_INTERSECT(ARRAY [''], ARRAY ['', NULL])", new ArrayType(createVarcharType(0)), ImmutableList.of(""));
        assertFunction("ARRAY_INTERSECT(ARRAY ['', NULL], ARRAY ['', NULL])", new ArrayType(createVarcharType(0)), asList("", null));
        assertFunction("ARRAY_INTERSECT(ARRAY [NULL], ARRAY ['abc', NULL])", new ArrayType(createVarcharType(3)), singletonList(null));
        assertFunction("ARRAY_INTERSECT(ARRAY ['abc', NULL, 'xyz', NULL], ARRAY [NULL, 'abc', NULL, NULL])", new ArrayType(createVarcharType(3)), asList("abc", null));
    }

    @Test
    public void testDuplicates()
    {
        assertFunction("array_intersect(ARRAY[1, 5, 3, 5, 1], ARRAY[3, 3, 5])", new ArrayType(INTEGER), ImmutableList.of(5, 3));
        assertFunction("array_intersect(ARRAY[CAST(1 as BIGINT), 5, 5, 3, 3, 3, 1], ARRAY[3, 5])", new ArrayType(BIGINT), ImmutableList.of(5L, 3L));
        assertFunction("array_intersect(ARRAY[CAST('x' as VARCHAR), 'x', 'y', 'z'], ARRAY['x', 'y', 'x'])", new ArrayType(VARCHAR), ImmutableList.of("x", "y"));
        assertFunction("array_intersect(ARRAY[true, false, null, true, false, null], ARRAY[true, true, true])", new ArrayType(BOOLEAN), asList(true));
    }

    @Test
    public void testSqlFunctions()
    {
        assertFunction("array_intersect(ARRAY[ARRAY[1, 3, 5], ARRAY[2, 3, 5], ARRAY[3, 3, 3, 6]])", new ArrayType(INTEGER), ImmutableList.of(3));
        assertFunction("array_intersect(ARRAY[null, ARRAY[], ARRAY[1, 2, 3]])", new ArrayType(INTEGER), null);
        assertFunction("array_intersect(ARRAY[ARRAY[], null, ARRAY[1, 2, 3]])", new ArrayType(INTEGER), null);
        assertFunction("array_intersect(ARRAY[])", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("array_intersect(null)", new ArrayType(UNKNOWN), null);
        assertFunction("array_intersect(ARRAY[ARRAY[], ARRAY[1, 2, 3]])", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("array_intersect(ARRAY[ARRAY[1, 2, 3], null])", new ArrayType(INTEGER), null);
        assertFunction("array_intersect(ARRAY[ARRAY[DOUBLE'1.1', DOUBLE'2.2', DOUBLE'3.3'], ARRAY[DOUBLE'1.1', DOUBLE'3.4'], ARRAY[DOUBLE'1.0', DOUBLE'1.1', DOUBLE'1.2']])", new ArrayType(DOUBLE), ImmutableList.of(1.1));

        assertFunction("array_intersect(ARRAY[ARRAY[ARRAY[1], ARRAY[2]], ARRAY[ARRAY[2], ARRAY[3]]])", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(2)));

        RowType rowType = RowType.from(ImmutableList.of(RowType.field("x", DOUBLE), RowType.field("y", DOUBLE)));
        String t = rowType.toString();
        assertFunction("array_intersect(ARRAY[ARRAY[CAST((1.0, 2.0) AS " + t + "), CAST((2.0, 3.0) AS " + t + ")], ARRAY[CAST((0.0, 1.0) AS " + t + "), CAST((1.0, 2.0) AS " + t + ")]])", new ArrayType(rowType), ImmutableList.of(ImmutableList.of(1.0, 2.0)));
    }
}
