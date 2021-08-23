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
import com.facebook.presto.common.function.Description;
import com.facebook.presto.common.function.OperatorDependency;
import com.facebook.presto.common.function.ScalarFunction;
import com.facebook.presto.common.function.SqlNullable;
import com.facebook.presto.common.function.SqlType;
import com.facebook.presto.common.function.TypeParameter;
import com.facebook.presto.common.type.Type;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.operator.scalar.ArrayMinMaxUtils.booleanArrayMinMax;
import static com.facebook.presto.operator.scalar.ArrayMinMaxUtils.doubleArrayMinMax;
import static com.facebook.presto.operator.scalar.ArrayMinMaxUtils.longArrayMinMax;
import static com.facebook.presto.operator.scalar.ArrayMinMaxUtils.sliceArrayMinMax;
import static com.facebook.presto.util.Failures.internalError;

@ScalarFunction("array_max")
@Description("Get maximum value of array")
public final class ArrayMaxFunction
{
    private ArrayMaxFunction() {}

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Long longArrayMax(
            @OperatorDependency(operator = GREATER_THAN, argumentTypes = {"T", "T"}) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        return longArrayMinMax(compareMethodHandle, elementType, block);
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Boolean booleanArrayMax(
            @OperatorDependency(operator = GREATER_THAN, argumentTypes = {"T", "T"}) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        return booleanArrayMinMax(compareMethodHandle, elementType, block);
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Double doubleArrayMax(
            @OperatorDependency(operator = GREATER_THAN, argumentTypes = {"T", "T"}) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        return doubleArrayMinMax(compareMethodHandle, elementType, block);
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Slice sliceArrayMax(
            @OperatorDependency(operator = GREATER_THAN, argumentTypes = {"T", "T"}) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        return sliceArrayMinMax(compareMethodHandle, elementType, block);
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Block blockArrayMax(
            @OperatorDependency(operator = GREATER_THAN, argumentTypes = {"T", "T"}) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        try {
            if (block.getPositionCount() == 0) {
                return null;
            }

            Block selectedValue = (Block) elementType.getObject(block, 0);
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    return null;
                }
                Block value = (Block) elementType.getObject(block, i);
                if ((boolean) compareMethodHandle.invokeExact(value, selectedValue)) {
                    selectedValue = value;
                }
            }

            return selectedValue;
        }
        catch (Throwable t) {
            throw internalError(t);
        }
    }
}
