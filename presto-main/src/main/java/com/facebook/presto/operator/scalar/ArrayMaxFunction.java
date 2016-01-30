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

import com.facebook.presto.operator.Description;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.metadata.OperatorType.GREATER_THAN;
import static com.facebook.presto.operator.scalar.ArrayMinMaxUtils.booleanArrayMinMax;
import static com.facebook.presto.operator.scalar.ArrayMinMaxUtils.doubleArrayMinMax;
import static com.facebook.presto.operator.scalar.ArrayMinMaxUtils.longArrayMinMax;
import static com.facebook.presto.operator.scalar.ArrayMinMaxUtils.sliceArrayMinMax;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.google.common.base.Throwables.propagateIfInstanceOf;

@ScalarFunction("array_max")
@Description("Get maximum value of array")
public final class ArrayMaxFunction
{
    private ArrayMaxFunction() {}

    @TypeParameter("T")
    @SqlType("T")
    @Nullable
    public static Void arrayWithUnknownType(
            @OperatorDependency(operator = GREATER_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle compareMethodHandle,
            @SqlType("array(T)") Block block)
    {
        return null;
    }

    @TypeParameter("T")
    @SqlType("T")
    @Nullable
    public static Long longArrayMax(
            @OperatorDependency(operator = GREATER_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        return longArrayMinMax(compareMethodHandle, elementType, block);
    }

    @TypeParameter("T")
    @SqlType("T")
    @Nullable
    public static Boolean booleanArrayMax(
            @OperatorDependency(operator = GREATER_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        return booleanArrayMinMax(compareMethodHandle, elementType, block);
    }

    @TypeParameter("T")
    @SqlType("T")
    @Nullable
    public static Double doubleArrayMax(
            @OperatorDependency(operator = GREATER_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        return doubleArrayMinMax(compareMethodHandle, elementType, block);
    }

    @TypeParameter("T")
    @SqlType("T")
    @Nullable
    public static Slice sliceArrayMax(
            @OperatorDependency(operator = GREATER_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        return sliceArrayMinMax(compareMethodHandle, elementType, block);
    }

    @TypeParameter("T")
    @SqlType("T")
    @Nullable
    public static Block blockArrayMax(
            @OperatorDependency(operator = GREATER_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle compareMethodHandle,
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
            propagateIfInstanceOf(t, Error.class);
            propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(INTERNAL_ERROR, t);
        }
    }
}
