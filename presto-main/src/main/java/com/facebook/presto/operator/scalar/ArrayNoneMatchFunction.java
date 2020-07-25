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
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.function.TypeParameterSpecialization;
import io.airlift.slice.Slice;

@Description("Returns true if all elements of the array don't match the given predicate")
@ScalarFunction(value = "none_match")
public final class ArrayNoneMatchFunction
{
    private ArrayNoneMatchFunction() {}

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Block.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean noneMatchBlock(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") BlockToBooleanFunction function)
    {
        Boolean anyMatchResult = ArrayAnyMatchFunction.anyMatchBlock(elementType, arrayBlock, function);
        if (anyMatchResult == null) {
            return null;
        }
        return !anyMatchResult;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Slice.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean noneMatchSlice(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") SliceToBooleanFunction function)
    {
        Boolean anyMatchResult = ArrayAnyMatchFunction.anyMatchSlice(elementType, arrayBlock, function);
        if (anyMatchResult == null) {
            return null;
        }
        return !anyMatchResult;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = long.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean noneMatchLong(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") LongToBooleanFunction function)
    {
        Boolean anyMatchResult = ArrayAnyMatchFunction.anyMatchLong(elementType, arrayBlock, function);
        if (anyMatchResult == null) {
            return null;
        }
        return !anyMatchResult;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = double.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean noneMatchDouble(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") DoubleToBooleanFunction function)
    {
        Boolean anyMatchResult = ArrayAnyMatchFunction.anyMatchDouble(elementType, arrayBlock, function);
        if (anyMatchResult == null) {
            return null;
        }
        return !anyMatchResult;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = boolean.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean noneMatchBoolean(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") BooleanToBooleanFunction function)
    {
        Boolean anyMatchResult = ArrayAnyMatchFunction.anyMatchBoolean(elementType, arrayBlock, function);
        if (anyMatchResult == null) {
            return null;
        }
        return !anyMatchResult;
    }
}
