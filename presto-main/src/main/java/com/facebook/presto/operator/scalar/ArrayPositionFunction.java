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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.function.OperatorType.EQUAL;

@Description("Returns the position of the first occurrence of the given value in array (or 0 if not found)")
@ScalarFunction("array_position")
public final class ArrayPositionFunction
        extends ArrayPositionWithIndexFunction
{
    private ArrayPositionFunction() {}

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long arrayPosition(
            @TypeParameter("T") Type type,
            @OperatorDependency(operator = EQUAL, argumentTypes = {"T", "T"}) MethodHandle equalMethodHandle,
            @SqlType("array(T)") Block array,
            @SqlType("T") boolean element)
    {
        return arrayPositionWithIndex(type, equalMethodHandle, array, element, 1);
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long arrayPosition(
            @TypeParameter("T") Type type,
            @OperatorDependency(operator = EQUAL, argumentTypes = {"T", "T"}) MethodHandle equalMethodHandle,
            @SqlType("array(T)") Block array,
            @SqlType("T") long element)
    {
        return arrayPositionWithIndex(type, equalMethodHandle, array, element, 1);
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long arrayPosition(
            @TypeParameter("T") Type type,
            @OperatorDependency(operator = EQUAL, argumentTypes = {"T", "T"}) MethodHandle equalMethodHandle,
            @SqlType("array(T)") Block array,
            @SqlType("T") double element)
    {
        return arrayPositionWithIndex(type, equalMethodHandle, array, element, 1);
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long arrayPosition(
            @TypeParameter("T") Type type,
            @OperatorDependency(operator = EQUAL, argumentTypes = {"T", "T"}) MethodHandle equalMethodHandle,
            @SqlType("array(T)") Block array,
            @SqlType("T") Slice element)
    {
        return arrayPositionWithIndex(type, equalMethodHandle, array, element, 1);
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long arrayPosition(
            @TypeParameter("T") Type type,
            @OperatorDependency(operator = EQUAL, argumentTypes = {"T", "T"}) MethodHandle equalMethodHandle,
            @SqlType("array(T)") Block array,
            @SqlType("T") Block element)
    {
        return arrayPositionWithIndex(type, equalMethodHandle, array, element, 1);
    }
}
