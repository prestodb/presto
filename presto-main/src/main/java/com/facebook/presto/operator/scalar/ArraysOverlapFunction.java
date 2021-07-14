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
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.function.TypeParameterSpecialization;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.util.Failures.internalError;

@ScalarFunction("arrays_overlap")
@Description("Returns true if arrays have common elements")
public final class ArraysOverlapFunction
{

    @TypeParameter("T")
    private ArraysOverlapFunction(@TypeParameter("T") Type elementType) {}

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Slice.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean arraysOverlapSlice(
            @OperatorDependency(operator = EQUAL, argumentTypes = {"T", "T"}) MethodHandle equals,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block leftArray,
            @SqlType("array(T)") Block rightArray)
    {
        if (leftArray.getPositionCount() == 0 || rightArray.getPositionCount() == 0) {
            return false;
        }
        boolean hasNull = false;
        for (int i = 0; i < leftArray.getPositionCount(); i++) {
            if (leftArray.isNull(i)) {
                hasNull = true;
                continue;
            }
            try {
                Slice leftValue = elementType.getSlice(leftArray, i);
                for (int j = 0; j < rightArray.getPositionCount(); j++) {
                    if (rightArray.isNull(j)) {
                        hasNull = true;
                        continue;
                    }
                    if ((Boolean) equals.invokeExact(elementType.getSlice(rightArray, j), leftValue)) {
                        return true;
                    }
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        if (hasNull) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Block.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean arraysOverlapBlock(
            @OperatorDependency(operator = EQUAL, argumentTypes = {"T", "T"}) MethodHandle equals,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block leftArray,
            @SqlType("array(T)") Block rightArray)
    {
        if (leftArray.getPositionCount() == 0 || rightArray.getPositionCount() == 0) {
            return false;
        }
        boolean hasNull = false;
        for (int i = 0; i < leftArray.getPositionCount(); i++) {
            if (leftArray.isNull(i)) {
                hasNull = true;
                continue;
            }
            try {
                Block leftValue = (Block) elementType.getObject(leftArray, i);
                for (int j = 0; j < rightArray.getPositionCount(); j++) {
                    if (rightArray.isNull(j)) {
                        hasNull = true;
                        continue;
                    }
                    if ((Boolean) equals.invokeExact((Block) elementType.getObject(rightArray, j), leftValue)) {
                        return true;
                    }
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        if (hasNull) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = long.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean arraysOverlapLong(
            @OperatorDependency(operator = EQUAL, argumentTypes = {"T", "T"}) MethodHandle equals,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block leftArray,
            @SqlType("array(T)") Block rightArray)
    {
        if (leftArray.getPositionCount() == 0 || rightArray.getPositionCount() == 0) {
            return false;
        }
        boolean hasNull = false;
        for (int i = 0; i < leftArray.getPositionCount(); i++) {
            if (leftArray.isNull(i)) {
                hasNull = true;
                continue;
            }
            try {
                Long leftValue = elementType.getLong(leftArray, i);
                for (int j = 0; j < rightArray.getPositionCount(); j++) {
                    if (rightArray.isNull(j)) {
                        hasNull = true;
                        continue;
                    }
                    if ((Boolean) equals.invokeExact(elementType.getLong(rightArray, j), leftValue.longValue())) {
                        return true;
                    }
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        if (hasNull) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = boolean.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean arraysOverlapBoolean(
            @OperatorDependency(operator = EQUAL, argumentTypes = {"T", "T"}) MethodHandle equals,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block leftArray,
            @SqlType("array(T)") Block rightArray)
    {
        if (leftArray.getPositionCount() == 0 || rightArray.getPositionCount() == 0) {
            return false;
        }
        boolean hasNull = false;
        for (int i = 0; i < leftArray.getPositionCount(); i++) {
            if (leftArray.isNull(i)) {
                hasNull = true;
                continue;
            }
            try {
                Boolean leftValue = elementType.getBoolean(leftArray, i);
                for (int j = 0; j < rightArray.getPositionCount(); j++) {
                    if (rightArray.isNull(j)) {
                        hasNull = true;
                        continue;
                    }
                    if ((Boolean) equals.invokeExact(elementType.getBoolean(rightArray, j), leftValue.booleanValue())) {
                        return true;
                    }
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        if (hasNull) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = double.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean arraysOverlapDouble(
            @OperatorDependency(operator = EQUAL, argumentTypes = {"T", "T"}) MethodHandle equals,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block leftArray,
            @SqlType("array(T)") Block rightArray)
    {
        if (leftArray.getPositionCount() == 0 || rightArray.getPositionCount() == 0) {
            return false;
        }
        boolean hasNull = false;
        for (int i = 0; i < leftArray.getPositionCount(); i++) {
            if (leftArray.isNull(i)) {
                hasNull = true;
                continue;
            }
            try {
                Double leftValue = elementType.getDouble(leftArray, i);
                for (int j = 0; j < rightArray.getPositionCount(); j++) {
                    if (rightArray.isNull(j)) {
                        hasNull = true;
                        continue;
                    }
                    if ((Boolean) equals.invokeExact(elementType.getDouble(rightArray, j), leftValue.doubleValue())) {
                        return true;
                    }
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        if (hasNull) {
            return null;
        }
        return false;
    }
}
