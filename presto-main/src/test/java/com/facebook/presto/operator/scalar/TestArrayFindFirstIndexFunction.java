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

public class TestArrayFindFirstIndexFunction
        extends AbstractTestFunctions
{
    @Test
    public void testBasic()
    {
        assertFunction("find_first_index(ARRAY [5, 6], x -> x = 5)", BIGINT, 1L);
        assertFunction("find_first_index(ARRAY [BIGINT '5', BIGINT '6'], x -> x = 5)", BIGINT, 1L);
        assertFunction("find_first_index(ARRAY [5, 6], x -> x > 5)", BIGINT, 2L);
        assertFunction("find_first_index(ARRAY [null, false, true, false, true, false], x -> nullif(x, false))", BIGINT, 3L);
        assertFunction("find_first_index(ARRAY [null, true, false, null, true, false, null], x -> not x)", BIGINT, 3L);
        assertFunction("find_first_index(ARRAY [4.8E0, 6.2E0], x -> x > 5)", BIGINT, 2L);
        assertFunction("find_first_index(ARRAY ['abc', 'def', 'ayz'], x -> substr(x, 1, 1) = 'a')", BIGINT, 1L);
        assertFunction("find_first_index(ARRAY [ARRAY ['abc', null, '123'], ARRAY ['def', 'x', '456']], x -> x[2] IS NULL)", BIGINT, 1L);
    }

    @Test
    public void testPositiveOffset()
    {
        assertFunction("find_first_index(ARRAY [5, 6], 2, x -> x = 5)", BIGINT, null);
        assertFunction("find_first_index(ARRAY [5, 6], 4, x -> x > 0)", BIGINT, null);
        assertFunction("find_first_index(ARRAY [5, 6, 7, 8], 3, x -> x > 5)", BIGINT, 3L);
        assertFunction("find_first_index(ARRAY [3, 4, 5, 6], 2, x -> x > 0)", BIGINT, 2L);
        assertFunction("find_first_index(ARRAY [3, 4, 5, 6], 2, x -> x < 4)", BIGINT, null);
        assertFunction("find_first_index(ARRAY [null, false, true, null, true, false], 4, x -> nullif(x, false))", BIGINT, 5L);
        assertFunction("find_first_index(ARRAY [4.8E0, 6.2E0, 7.8E0], 3, x -> x > 5)", BIGINT, 3L);
        assertFunction("find_first_index(ARRAY ['abc', 'def', 'ayz'], 2, x -> substr(x, 1, 1) = 'a')", BIGINT, 3L);
        assertFunction("find_first_index(ARRAY [ARRAY ['abc', null, '123'], ARRAY ['def', null, '456']], 2, x -> x[2] IS NULL)", BIGINT, 2L);
    }

    @Test
    public void testNegativeOffset()
    {
        assertFunction("find_first_index(ARRAY [5, 6], -2, x -> x > 5)", BIGINT, null);
        assertFunction("find_first_index(ARRAY [5, 6], -4, x -> x > 0)", BIGINT, null);
        assertFunction("find_first_index(ARRAY [5, 6, 7, 8], -2, x -> x > 5)", BIGINT, 3L);
        assertFunction("find_first_index(ARRAY [9, 6, 3, 8], -2, x -> x > 5)", BIGINT, 2L);
        assertFunction("find_first_index(ARRAY [3, 4, 5, 6], -2, x -> x > 0)", BIGINT, 3L);
        assertFunction("find_first_index(ARRAY [3, 4, 5, 6], -2, x -> x > 5)", BIGINT, null);
        assertFunction("find_first_index(ARRAY [null, false, true, null, true, false], -3, x -> nullif(x, false))", BIGINT, 3L);
        assertFunction("find_first_index(ARRAY [4.8E0, 6.2E0, 7.8E0], -2, x -> x > 5)", BIGINT, 2L);
        assertFunction("find_first_index(ARRAY ['abc', 'def', 'ayz'], -2, x -> substr(x, 1, 1) = 'a')", BIGINT, 1L);
        assertFunction("find_first_index(ARRAY [ARRAY ['abc', null, '123'], ARRAY ['def', null, '456']], -2, x -> x[2] IS NULL)", BIGINT, 1L);
    }

    @Test
    public void testEmpty()
    {
        assertFunction("find_first_index(ARRAY [], x -> true)", BIGINT, null);
        assertFunction("find_first_index(ARRAY [], x -> false)", BIGINT, null);
        assertFunction("find_first_index(CAST (ARRAY [] AS ARRAY(BIGINT)), x -> true)", BIGINT, null);
    }

    @Test
    public void testNullArray()
    {
        assertFunction("find_first_index(ARRAY [NULL], x -> x IS NULL)", BIGINT, 1L);
        assertFunction("find_first_index(ARRAY [NULL], x -> x IS NOT NULL)", BIGINT, null);
        assertFunction("find_first_index(ARRAY [CAST (NULL AS BIGINT)], x -> x IS NULL)", BIGINT, 1L);
    }

    @Test
    public void testNull()
    {
        assertFunction("find_first_index(NULL, x -> true)", BIGINT, null);
        assertFunction("find_first_index(NULL, x -> false)", BIGINT, null);
    }
}
