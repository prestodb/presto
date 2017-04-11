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

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class TestArrayExceptFunction
        extends AbstractTestFunctions
{
    @Test
    public void testBasic()
            throws Exception
    {
        assertFunction("array_except(ARRAY[1, 5, 3], ARRAY[3])", new ArrayType(INTEGER), ImmutableList.of(1, 5));
        assertFunction("array_except(ARRAY[CAST(1 as BIGINT), 5, 3], ARRAY[5])", new ArrayType(BIGINT), ImmutableList.of(1L, 3L));
        assertFunction("array_except(ARRAY[CAST('x' as VARCHAR), 'y', 'z'], ARRAY['x'])", new ArrayType(VARCHAR), ImmutableList.of("y", "z"));
        assertFunction("array_except(ARRAY[true, false, null], ARRAY[true])", new ArrayType(BOOLEAN), asList(false, null));
        assertFunction("array_except(ARRAY[1.1, 5.4, 3.9], ARRAY[5, 5.4])", new ArrayType(DOUBLE), ImmutableList.of(1.1, 3.9));
    }

    @Test
    public void testEmpty()
            throws Exception
    {
        assertFunction("array_except(ARRAY[], ARRAY[])", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("array_except(ARRAY[], ARRAY[1, 3])", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("array_except(ARRAY[CAST('abc' as VARCHAR)], ARRAY[])", new ArrayType(VARCHAR), ImmutableList.of("abc"));
    }

    @Test
    public void testNull()
            throws Exception
    {
        assertFunction("array_except(ARRAY[NULL], NULL)", new ArrayType(UNKNOWN), null);
        assertFunction("array_except(NULL, NULL)", new ArrayType(UNKNOWN), null);
        assertFunction("array_except(NULL, ARRAY[NULL])", new ArrayType(UNKNOWN), null);
        assertFunction("array_except(ARRAY[NULL], ARRAY[NULL])", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("array_except(ARRAY[], ARRAY[NULL])", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("array_except(ARRAY[NULL], ARRAY[])", new ArrayType(UNKNOWN), singletonList(null));
    }

    @Test
    public void testDuplicates()
            throws Exception
    {
        assertFunction("array_except(ARRAY[1, 5, 3, 5, 1], ARRAY[3])", new ArrayType(INTEGER), ImmutableList.of(1, 5));
        assertFunction("array_except(ARRAY[CAST(1 as BIGINT), 5, 5, 3, 3, 3, 1], ARRAY[3, 5])", new ArrayType(BIGINT), ImmutableList.of(1L));
        assertFunction("array_except(ARRAY[CAST('x' as VARCHAR), 'x', 'y', 'z'], ARRAY['x', 'y', 'x'])", new ArrayType(VARCHAR), ImmutableList.of("z"));
        assertFunction("array_except(ARRAY[true, false, null, true, false, null], ARRAY[true, true, true])", new ArrayType(BOOLEAN), asList(false, null));
    }
}
