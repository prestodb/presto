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

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.util.Failures.internalError;

public final class ArrayMinMaxUtils
{
    private ArrayMinMaxUtils() {}

    @UsedByGeneratedCode
    public static Long longArrayMinMax(MethodHandle compareMethodHandle, Type elementType, Block block)
    {
        try {
            if (block.getPositionCount() == 0) {
                return null;
            }

            long selectedValue = elementType.getLong(block, 0);
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    return null;
                }
                long value = elementType.getLong(block, i);
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

    @UsedByGeneratedCode
    public static Boolean booleanArrayMinMax(MethodHandle compareMethodHandle, Type elementType, Block block)
    {
        try {
            if (block.getPositionCount() == 0) {
                return null;
            }

            boolean selectedValue = elementType.getBoolean(block, 0);
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    return null;
                }
                boolean value = elementType.getBoolean(block, i);
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

    @UsedByGeneratedCode
    public static Double doubleArrayMinMax(MethodHandle compareMethodHandle, Type elementType, Block block)
    {
        try {
            if (block.getPositionCount() == 0) {
                return null;
            }

            boolean containNull = false;
            double selectedValue = elementType.getDouble(block, 0);
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    containNull = true;
                }
                double value = elementType.getDouble(block, i);
                if ((boolean) compareMethodHandle.invokeExact(value, selectedValue)) {
                    selectedValue = value;
                }
            }

            return containNull ? null : selectedValue;
        }
        catch (Throwable t) {
            throw internalError(t);
        }
    }

    @UsedByGeneratedCode
    public static Slice sliceArrayMinMax(MethodHandle compareMethodHandle, Type elementType, Block block)
    {
        try {
            if (block.getPositionCount() == 0) {
                return null;
            }

            Slice selectedValue = elementType.getSlice(block, 0);
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    return null;
                }
                Slice value = elementType.getSlice(block, i);
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
