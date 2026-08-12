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
package com.facebook.presto.nativetests.operator.scalar;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.tests.operator.scalar.AbstractTestArrayExcept;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class TestArrayExceptFunction
        extends AbstractTestNativeFunctions
        implements AbstractTestArrayExcept
{
    @Test
    public void testEmpty()
    {
        assertInvalidFunction("array_except(ARRAY[], ARRAY[])", GENERIC_INTERNAL_ERROR, "not a known type kind: UNKNOWN");
        assertFunction("array_except(ARRAY[], ARRAY[1, 3])", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("array_except(ARRAY[CAST('abc' as VARCHAR)], ARRAY[])", new ArrayType(VARCHAR), ImmutableList.of("abc"));
    }

    // Velox's type dispatch macros (VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH) do not
    // handle TypeKind::UNKNOWN. Expressions with UNKNOWN-typed arrays produce a
    // GENERIC_INTERNAL_ERROR from the sidecar instead of being evaluated.
    @Test
    public void testNull()
    {
        assertInvalidFunction("array_except(ARRAY[], ARRAY[])", GENERIC_INTERNAL_ERROR, "not a known type kind: UNKNOWN");
        assertInvalidFunction("array_except(ARRAY[NULL], NULL)", GENERIC_INTERNAL_ERROR, "not a known type kind: UNKNOWN");
        assertInvalidFunction("array_except(NULL, NULL)", GENERIC_INTERNAL_ERROR, "not a known type kind: UNKNOWN");
        assertInvalidFunction("array_except(NULL, ARRAY[NULL])", GENERIC_INTERNAL_ERROR, "not a known type kind: UNKNOWN");
        assertInvalidFunction("array_except(ARRAY[NULL], ARRAY[NULL])", GENERIC_INTERNAL_ERROR, "not a known type kind: UNKNOWN");
        assertInvalidFunction("array_except(ARRAY[], ARRAY[NULL])", GENERIC_INTERNAL_ERROR, "not a known type kind: UNKNOWN");
        assertInvalidFunction("array_except(ARRAY[NULL], ARRAY[])", GENERIC_INTERNAL_ERROR, "not a known type kind: UNKNOWN");
    }
}
