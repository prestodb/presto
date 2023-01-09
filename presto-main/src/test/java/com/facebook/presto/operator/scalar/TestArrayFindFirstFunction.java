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
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static java.util.Arrays.asList;

public class TestArrayFindFirstFunction
        extends AbstractTestFunctions
{
    @Test
    public void testBasic()
    {
        assertFunction("find_first(ARRAY [5, 6], x -> x = 5)", INTEGER, 5);
        assertFunction("find_first(ARRAY [BIGINT '5', BIGINT '6'], x -> x = 5)", BIGINT, 5L);
        assertFunction("find_first(ARRAY [5, 6], x -> x > 5)", INTEGER, 6);
        assertFunction("find_first(ARRAY [null, false, true, false, true, false], x -> nullif(x, false))", BOOLEAN, true);
        assertFunction("find_first(ARRAY [null, true, false, null, true, false, null], x -> not x)", BOOLEAN, false);
        assertFunction("find_first(ARRAY [4.8E0, 6.2E0], x -> x > 5)", DOUBLE, 6.2);
        assertFunction("find_first(ARRAY ['abc', 'def', 'ayz'], x -> substr(x, 1, 1) = 'a')", createVarcharType(3), "abc");
        assertFunction(
                "find_first(ARRAY [ARRAY ['abc', null, '123'], ARRAY ['def', 'x', '456']], x -> x[2] IS NULL)",
                new ArrayType(createVarcharType(3)),
                asList("abc", null, "123"));
    }

    @Test
    public void testPositiveOffset()
    {
        assertFunction("find_first(ARRAY [5, 6], 2, x -> x = 5)", INTEGER, null);
        assertFunction("find_first(ARRAY [5, 6], 4, x -> x > 0)", INTEGER, null);
        assertFunction("find_first(ARRAY [5, 6, 7, 8], 3, x -> x > 5)", INTEGER, 7);
        assertFunction("find_first(ARRAY [3, 4, 5, 6], 2, x -> x > 0)", INTEGER, 4);
        assertFunction("find_first(ARRAY [3, 4, 5, 6], 2, x -> x < 4)", INTEGER, null);
        assertFunction("find_first(ARRAY [null, false, true, null, true, false], 4, x -> nullif(x, false))", BOOLEAN, true);
        assertFunction("find_first(ARRAY [4.8E0, 6.2E0, 7.8E0], 3, x -> x > 5)", DOUBLE, 7.8);
        assertFunction("find_first(ARRAY ['abc', 'def', 'ayz'], 2, x -> substr(x, 1, 1) = 'a')", createVarcharType(3), "ayz");
        assertFunction(
                "find_first(ARRAY [ARRAY ['abc', null, '123'], ARRAY ['def', null, '456']], 2, x -> x[2] IS NULL)",
                new ArrayType(createVarcharType(3)),
                asList("def", null, "456"));
    }

    @Test
    public void testNegativeOffset()
    {
        assertFunction("find_first(ARRAY [5, 6], -2, x -> x > 5)", INTEGER, null);
        assertFunction("find_first(ARRAY [5, 6], -4, x -> x > 0)", INTEGER, null);
        assertFunction("find_first(ARRAY [5, 6, 7, 8], -2, x -> x > 5)", INTEGER, 7);
        assertFunction("find_first(ARRAY [9, 6, 3, 8], -2, x -> x > 5)", INTEGER, 6);
        assertFunction("find_first(ARRAY [3, 4, 5, 6], -2, x -> x > 0)", INTEGER, 5);
        assertFunction("find_first(ARRAY [3, 4, 5, 6], -2, x -> x > 5)", INTEGER, null);
        assertFunction("find_first(ARRAY [null, false, true, null, true, false], -3, x -> nullif(x, false))", BOOLEAN, true);
        assertFunction("find_first(ARRAY [4.8E0, 6.2E0, 7.8E0], -2, x -> x > 5)", DOUBLE, 6.2);
        assertFunction("find_first(ARRAY ['abc', 'def', 'ayz'], -2, x -> substr(x, 1, 1) = 'a')", createVarcharType(3), "abc");
        assertFunction(
                "find_first(ARRAY [ARRAY ['abc', null, '123'], ARRAY ['def', null, '456']], -2, x -> x[2] IS NULL)",
                new ArrayType(createVarcharType(3)),
                asList("abc", null, "123"));
    }

    @Test
    public void testEmpty()
    {
        assertFunction("find_first(ARRAY [], x -> true)", UNKNOWN, null);
        assertFunction("find_first(ARRAY [], x -> false)", UNKNOWN, null);
        assertFunction("find_first(CAST (ARRAY [] AS ARRAY(INTEGER)), x -> true)", INTEGER, null);
    }

    @Test
    public void testNullArray()
    {
        assertFunction("find_first(ARRAY [NULL], x -> x IS NULL)", UNKNOWN, null);
        assertFunction("find_first(ARRAY [NULL], x -> x IS NOT NULL)", UNKNOWN, null);
        assertFunction("find_first(ARRAY [CAST (NULL AS INTEGER)], x -> x IS NULL)", INTEGER, null);
    }

    @Test
    public void testNull()
    {
        assertFunction("find_first(NULL, x -> true)", UNKNOWN, null);
        assertFunction("find_first(NULL, x -> false)", UNKNOWN, null);
    }
}
