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
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.util.Arrays.asList;

public class TestArrayIntersectFunction
        extends AbstractTestFunctions
{
    @Test
    public void testBasic()
    {
        assertFunction("array_intersect(ARRAY[1, 5, 3], ARRAY[3])", new ArrayType(INTEGER), ImmutableList.of(3));
        assertFunction("array_intersect(ARRAY[CAST(1 as BIGINT), 5, 3], ARRAY[5])", new ArrayType(BIGINT), ImmutableList.of(5L));
        assertFunction("array_intersect(ARRAY[CAST('x' as VARCHAR), 'y', 'z'], ARRAY['x', 'y'])", new ArrayType(VARCHAR), ImmutableList.of("x", "y"));
        assertFunction("array_intersect(ARRAY[true, false, null], ARRAY[true, null])", new ArrayType(BOOLEAN), asList(true, null));
        assertFunction("array_intersect(ARRAY[1.1E0, 5.4E0, 3.9E0], ARRAY[5, 5.4E0])", new ArrayType(DOUBLE), ImmutableList.of(5.4));
    }

    @Test
    public void testEmpty()
    {
        assertFunction("array_intersect(ARRAY[], ARRAY[])", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("array_intersect(ARRAY[], ARRAY[1, 3])", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("array_intersect(ARRAY[CAST('abc' as VARCHAR)], ARRAY[])", new ArrayType(VARCHAR), ImmutableList.of());
    }

    @Test
    public void testNull()
    {
        assertFunction("array_intersect(ARRAY[NULL], NULL)", new ArrayType(UNKNOWN), null);
        assertFunction("array_intersect(NULL, NULL)", new ArrayType(UNKNOWN), null);
        assertFunction("array_intersect(NULL, ARRAY[NULL])", new ArrayType(UNKNOWN), null);
        assertFunction("array_intersect(ARRAY[NULL], ARRAY[NULL])", new ArrayType(UNKNOWN), asList(false ? 1 : null));
        assertFunction("array_intersect(ARRAY[], ARRAY[NULL])", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("array_intersect(ARRAY[NULL], ARRAY[])", new ArrayType(UNKNOWN), ImmutableList.of());
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
    public void testSQLFunctions()
    {
        assertFunction("array_intersect(ARRAY[ARRAY[1, 3, 5], ARRAY[2, 3, 5], ARRAY[3, 3, 3, 6]])", new ArrayType(INTEGER), ImmutableList.of(3));
        assertFunction("array_intersect(ARRAY[ARRAY[], ARRAY[1, 2, 3]])", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("array_intersect(ARRAY[ARRAY[1, 2, 3], null])", new ArrayType(INTEGER), null);
        assertFunction("array_intersect(ARRAY[ARRAY[DOUBLE'1.1', DOUBLE'2.2', DOUBLE'3.3'], ARRAY[DOUBLE'1.1', DOUBLE'3.4'], ARRAY[DOUBLE'1.0', DOUBLE'1.1', DOUBLE'1.2']])", new ArrayType(DOUBLE), ImmutableList.of(1.1));

        assertFunction("array_intersect(ARRAY[ARRAY[ARRAY[1], ARRAY[2]], ARRAY[ARRAY[2], ARRAY[3]]])", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(2)));

        RowType rowType = RowType.from(ImmutableList.of(RowType.field("x", DOUBLE), RowType.field("y", DOUBLE)));
        String t = rowType.toString();
        assertFunction("array_intersect(ARRAY[ARRAY[CAST((1.0, 2.0) AS " + t + "), CAST((2.0, 3.0) AS " + t + ")], ARRAY[CAST((0.0, 1.0) AS " + t + "), CAST((1.0, 2.0) AS " + t + ")]])", new ArrayType(rowType), ImmutableList.of(ImmutableList.of(1.0, 2.0)));
    }
}
