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

import static com.facebook.presto.common.type.BigintType.BIGINT;

public class TestArraysOverlapCountFunction
        extends AbstractTestFunctions
{
    @Test
    public void testVarchar()
    {
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY[CAST('x' as VARCHAR), 'y', 'z'], ARRAY['x', 'y'])", BIGINT, 2L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY ['abc'], ARRAY ['bcd'])", BIGINT, 0L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY ['abc'], ARRAY ['abc', 'bcd'])", BIGINT, 1L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY ['abc', 'bcd'], ARRAY ['abc', 'bcd'])", BIGINT, 2L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY ['abc', 'abc'], ARRAY ['abc', 'abc'])", BIGINT, 1L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY ['foo', 'bar', 'baz'], ARRAY ['foo', 'test', 'bar'])", BIGINT, 2L);
    }

    @Test
    public void testBigint()
    {
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY[CAST(3 AS BIGINT)], ARRAY[CAST(5 AS BIGINT)])", BIGINT, 0L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY[CAST(1 as BIGINT), 5, 3], ARRAY[5])", BIGINT, 1L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [CAST(5 AS BIGINT), CAST(5 AS BIGINT)], ARRAY [CAST(1 AS BIGINT), CAST(5 AS BIGINT)])", BIGINT, 1L);
    }

    @Test
    public void testInteger()
    {
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY[1, 5, 3], ARRAY[3])", BIGINT, 1L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [5], ARRAY [5])", BIGINT, 1L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [3], ARRAY [5])", BIGINT, 0L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [1, 2, 5, 5, 6], ARRAY [5, 5, 6, 6, 7, 8])", BIGINT, 2L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [IF (RAND() < 1.0E0, 7, 1) , 2], ARRAY [7])", BIGINT, 1L);
    }

    @Test
    public void testDouble()
    {
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [5.0E0], ARRAY [1.0E0])", BIGINT, 0L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [1, 5], ARRAY [1.0E0])", BIGINT, 1L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [1.0E0, 5.0E0], ARRAY [5.0E0, 5.0E0, 6.0E0])", BIGINT, 1L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [8.3E0, 1.6E0, 4.1E0, 5.2E0], ARRAY [4.0E0, 5.2E0, 8.3E0, 9.7E0, 3.5E0])", BIGINT, 2L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [5.1E0, 7, 3.0E0, 4.8E0, 10], ARRAY [6.5E0, 10.0E0, 1.9E0, 5.1E0, 3.9E0, 4.8E0])", BIGINT, 3L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY[1.1E0, 5.4E0, 3.9E0], ARRAY[5, 5.4E0])", BIGINT, 1L);
    }

    @Test
    public void testDecimal()
    {
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [2.3], ARRAY[2.2])", BIGINT, 0L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [2.3, 2.3, 2.2], ARRAY[2.2, 2.3])", BIGINT, 2L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [2.330, 1.900, 2.330], ARRAY [2.3300, 1.9000])", BIGINT, 2L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [2, 3], ARRAY[2.0, 3.0])", BIGINT, 2L);
    }

    @Test
    public void testBoolean()
    {
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [true], ARRAY [false])", BIGINT, 0L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [true], ARRAY [true])", BIGINT, 1L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [true, false], ARRAY [true])", BIGINT, 1L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [true, true], ARRAY [true, true])", BIGINT, 1L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY[true, false, null], ARRAY[true, null])", BIGINT, 2L);
    }

    @Test
    public void testRow()
    {
        assertFunction(
                "ARRAYS_OVERLAP_COUNT(ARRAY[(123, 456), (123, 789)], ARRAY[(123, 456), (123, 456), (123, 789)])",
                BIGINT,
                2L);
        assertFunction(
                "ARRAYS_OVERLAP_COUNT(ARRAY[ARRAY[123, 456], ARRAY[123, 789]], ARRAY[ARRAY[123, 456], ARRAY[123, 456], ARRAY[123, 789]])",
                BIGINT,
                2L);
        assertFunction(
                "ARRAYS_OVERLAP_COUNT(ARRAY[(123, 'abc'), (123, 'cde')], ARRAY[(123, 'abc'), (123, 'cde')])",
                BIGINT,
                2L);
        assertFunction(
                "ARRAYS_OVERLAP_COUNT(ARRAY[(123, 'abc'), (123, 'cde'), NULL], ARRAY[(123, 'abc'), (123, 'cde')])",
                BIGINT,
                2L);
        assertFunction(
                "ARRAYS_OVERLAP_COUNT(ARRAY[(123, 'abc'), (123, 'cde'), NULL, NULL], ARRAY[(123, 'abc'), (123, 'cde'), NULL])",
                BIGINT,
                3L);
        assertFunction(
                "ARRAYS_OVERLAP_COUNT(ARRAY[(123, 'abc'), (123, 'abc')], ARRAY[(123, 'abc'), (123, NULL)])",
                BIGINT,
                1L);
        assertFunction(
                "ARRAYS_OVERLAP_COUNT(ARRAY[(123, 'abc')], ARRAY[(123, NULL)])",
                BIGINT,
                0L);
    }

    @Test
    public void testIndeterminateRows()
    {
        assertFunction(
                "ARRAYS_OVERLAP_COUNT(ARRAY[(123, 'abc'), (123, NULL)], ARRAY[(123, 'abc'), (123, NULL)])",
                BIGINT,
                2L);
        assertFunction(
                "ARRAYS_OVERLAP_COUNT(ARRAY[(NULL, 'abc'), (123, 'abc')], ARRAY[(123, 'abc'),(NULL, 'abc')])",
                BIGINT,
                2L);
    }

    @Test
    public void testIndeterminateArrays()
    {
        assertFunction(
                "ARRAYS_OVERLAP_COUNT(ARRAY[ARRAY[123, 456], ARRAY[123, NULL]], ARRAY[ARRAY[123, 456], ARRAY[123, NULL]])",
                BIGINT,
                2L);
        assertFunction(
                "ARRAYS_OVERLAP_COUNT(ARRAY[ARRAY[NULL, 456], ARRAY[123, 456]], ARRAY[ARRAY[123, 456],ARRAY[NULL, 456]])",
                BIGINT,
                2L);
    }

    @Test
    public void testUnboundedRetainedSize()
    {
        assertCachedInstanceHasBoundedRetainedSize("ARRAYS_OVERLAP_COUNT(ARRAY ['foo', 'bar', 'baz'], ARRAY ['foo', 'test', 'bar'])");
    }

    @Test
    public void testEmptyArrays()
    {
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY[], ARRAY[])", BIGINT, 0L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY[], ARRAY[1, 3])", BIGINT, 0L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [], ARRAY [5])", BIGINT, 0L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [5, 6], ARRAY [])", BIGINT, 0L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY ['abc'], ARRAY [])", BIGINT, 0L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [], ARRAY ['abc', 'bcd'])", BIGINT, 0L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [], ARRAY [NULL])", BIGINT, 0L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [], ARRAY [false])", BIGINT, 0L);
    }

    @Test
    public void testEmptyResults()
    {
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [1], ARRAY [5])", BIGINT, 0L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [CAST(1 AS BIGINT)], ARRAY [CAST(5 AS BIGINT)])", BIGINT, 0L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [true, true], ARRAY [false])", BIGINT, 0L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [5], ARRAY [1.0E0])", BIGINT, 0L);
    }

    @Test
    public void testNull()
    {
        assertFunction("ARRAYS_OVERLAP_COUNT(NULL, NULL)", BIGINT, null);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY[NULL], NULL)", BIGINT, null);
        assertFunction("ARRAYS_OVERLAP_COUNT(NULL, ARRAY[NULL])", BIGINT, null);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY[NULL], ARRAY[NULL])", BIGINT, 1L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY[], ARRAY[NULL])", BIGINT, 0L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY[NULL], ARRAY[])", BIGINT, 0L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [NULL], ARRAY [NULL, NULL])", BIGINT, 1L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [0, 0, 1, NULL], ARRAY [0, 0, 1, NULL])", BIGINT, 3L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [0, 0], ARRAY [0, 0, NULL])", BIGINT, 1L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [CAST(0 AS BIGINT), CAST(0 AS BIGINT)], ARRAY [CAST(0 AS BIGINT), NULL])", BIGINT, 1L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [0.0E0], ARRAY [NULL])", BIGINT, 0L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [0.0E0, NULL], ARRAY [0.0E0, NULL])", BIGINT, 2L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [true, true, false, false, NULL], ARRAY [true, false, false, NULL])", BIGINT, 3L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [false, false], ARRAY [false, false, NULL])", BIGINT, 1L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY ['abc'], ARRAY [NULL])", BIGINT, 0L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [''], ARRAY ['', NULL])", BIGINT, 1L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY ['', NULL], ARRAY ['', NULL])", BIGINT, 2L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY [NULL], ARRAY ['abc', NULL])", BIGINT, 1L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY ['abc', NULL, 'xyz', NULL], ARRAY [NULL, 'abc', NULL, NULL])", BIGINT, 2L);
    }

    @Test
    public void testDuplicates()
    {
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY[1, 5, 3, 5, 1], ARRAY[3, 3, 5])", BIGINT, 2L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY[CAST(1 as BIGINT), 5, 5, 3, 3, 3, 1], ARRAY[3, 5])", BIGINT, 2L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY[CAST('x' as VARCHAR), 'x', 'y', 'z'], ARRAY['x', 'y', 'x'])", BIGINT, 2L);
        assertFunction("ARRAYS_OVERLAP_COUNT(ARRAY[true, false, null, true, false, null], ARRAY[true, true, true])", BIGINT, 1L);
    }
}
