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
import com.facebook.presto.tests.operator.scalar.AbstractTestArrayExcept;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.util.Collections.singletonList;

public class TestArrayExceptFunction
        extends AbstractTestFunctions
        implements AbstractTestArrayExcept
{
    @Test
    void testEmpty()
    {
        assertFunction("array_except(ARRAY[], ARRAY[])", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("array_except(ARRAY[], ARRAY[1, 3])", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("array_except(ARRAY[CAST('abc' as VARCHAR)], ARRAY[])", new ArrayType(VARCHAR), ImmutableList.of("abc"));
    }

    @Test
    public void testNull()
    {
        assertFunction("array_except(ARRAY[NULL], NULL)", new ArrayType(UNKNOWN), null);
        assertFunction("array_except(NULL, NULL)", new ArrayType(UNKNOWN), null);
        assertFunction("array_except(NULL, ARRAY[NULL])", new ArrayType(UNKNOWN), null);
        assertFunction("array_except(ARRAY[NULL], ARRAY[NULL])", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("array_except(ARRAY[], ARRAY[NULL])", new ArrayType(UNKNOWN), ImmutableList.of());
        assertFunction("array_except(ARRAY[NULL], ARRAY[])", new ArrayType(UNKNOWN), singletonList(null));
    }
}
