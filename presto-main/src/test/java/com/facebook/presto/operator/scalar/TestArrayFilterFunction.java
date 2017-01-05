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

import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class TestArrayFilterFunction
        extends AbstractTestFunctions
{
    @Test
    public void testBasic()
            throws Exception
    {
        assertFunction("filter(ARRAY [5, 6], x -> x = 5)", new ArrayType(INTEGER), ImmutableList.of(5));
        assertFunction("filter(ARRAY [5 + RANDOM(1), 6 + RANDOM(1)], x -> x = 5)", new ArrayType(INTEGER), ImmutableList.of(5));
        assertFunction("filter(ARRAY [true, false, true, false], x -> nullif(x, false))", new ArrayType(BOOLEAN), ImmutableList.of(true, true));
        assertFunction("filter(ARRAY [true, false, null, true, false, null], x -> not x)", new ArrayType(BOOLEAN), ImmutableList.of(false, false));
    }

    @Test
    public void testEmpty()
            throws Exception
    {
        assertFunction("filter(ARRAY [], x -> true)", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("filter(ARRAY [], x -> false)", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("filter(ARRAY [], x -> CAST (null AS BOOLEAN))", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("filter(CAST (ARRAY [] AS ARRAY(INTEGER)), x -> true)", new ArrayType(INTEGER), ImmutableList.of());
    }

    @Test
    public void testNull()
            throws Exception
    {
        assertFunction("filter(ARRAY [NULL], x -> x IS NULL)", new ArrayType(UNKNOWN), singletonList(null));
        assertFunction("filter(ARRAY [NULL], x -> x IS NOT NULL)", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("filter(ARRAY [CAST (NULL AS INTEGER)], x -> x IS NULL)", new ArrayType(INTEGER), singletonList(null));
        assertFunction("filter(ARRAY [NULL, NULL, NULL], x -> x IS NULL)", new ArrayType(UNKNOWN), asList(null, null, null));
        assertFunction("filter(ARRAY [NULL, NULL, NULL], x -> x IS NOT NULL)", new ArrayType(UNKNOWN), ImmutableList.of());
    }

    @Test
    public void testTypeCombinations()
            throws Exception
    {
        assertFunction("filter(ARRAY [25, 26, 27], x -> x % 2 = 1)", new ArrayType(INTEGER), ImmutableList.of(25, 27));
        assertFunction("filter(ARRAY [25.6, 37.3, 28.6], x -> x < 30.0)", new ArrayType(DOUBLE), ImmutableList.of(25.6, 28.6));
        assertFunction("filter(ARRAY [true, false, true], x -> not x)", new ArrayType(BOOLEAN), ImmutableList.of(false));
        assertFunction("filter(ARRAY ['abc', 'def', 'ayz'], x -> substr(x, 1, 1) = 'a')", new ArrayType(createVarcharType(3)), ImmutableList.of("abc", "ayz"));
        assertFunction(
                "filter(ARRAY [ARRAY ['abc', null, '123'], ARRAY ['def', 'x', '456']], x -> x[2] IS NULL)",
                new ArrayType(new ArrayType(createVarcharType(3))),
                ImmutableList.of(asList("abc", null, "123")));
    }
}
