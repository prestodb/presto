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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.function.TypeParameterSpecialization;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static java.lang.Boolean.TRUE;

@Description("return array containing elements that match the given predicate")
@ScalarFunction(value = "filter", deterministic = false)
public final class ArrayFilterFunction
{
    private ArrayFilterFunction() {}

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = long.class)
    @SqlType("array(T)")
    public static Block filterLong(@TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") MethodHandle function)
    {
        int positionCount = arrayBlock.getPositionCount();
        BlockBuilder resultBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), positionCount);
        for (int position = 0; position < positionCount; position++) {
            Long input = null;
            if (!arrayBlock.isNull(position)) {
                input = elementType.getLong(arrayBlock, position);
            }

            Boolean keep;
            try {
                keep = (Boolean) function.invokeExact(input);
            }
            catch (Throwable throwable) {
                throw Throwables.propagate(throwable);
            }
            if (TRUE.equals(keep)) {
                elementType.appendTo(arrayBlock, position, resultBuilder);
            }
        }
        return resultBuilder.build();
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = double.class)
    @SqlType("array(T)")
    public static Block filterDouble(@TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") MethodHandle function)
    {
        int positionCount = arrayBlock.getPositionCount();
        BlockBuilder resultBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), positionCount);
        for (int position = 0; position < positionCount; position++) {
            Double input = null;
            if (!arrayBlock.isNull(position)) {
                input = elementType.getDouble(arrayBlock, position);
            }

            Boolean keep;
            try {
                keep = (Boolean) function.invokeExact(input);
            }
            catch (Throwable throwable) {
                throw Throwables.propagate(throwable);
            }
            if (TRUE.equals(keep)) {
                elementType.appendTo(arrayBlock, position, resultBuilder);
            }
        }
        return resultBuilder.build();
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = boolean.class)
    @SqlType("array(T)")
    public static Block filterBoolean(@TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") MethodHandle function)
    {
        int positionCount = arrayBlock.getPositionCount();
        BlockBuilder resultBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), positionCount);
        for (int position = 0; position < positionCount; position++) {
            Boolean input = null;
            if (!arrayBlock.isNull(position)) {
                input = elementType.getBoolean(arrayBlock, position);
            }

            Boolean keep;
            try {
                keep = (Boolean) function.invokeExact(input);
            }
            catch (Throwable throwable) {
                throw Throwables.propagate(throwable);
            }
            if (TRUE.equals(keep)) {
                elementType.appendTo(arrayBlock, position, resultBuilder);
            }
        }
        return resultBuilder.build();
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Slice.class)
    @SqlType("array(T)")
    public static Block filterSlice(@TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") MethodHandle function)
    {
        int positionCount = arrayBlock.getPositionCount();
        BlockBuilder resultBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), positionCount);
        for (int position = 0; position < positionCount; position++) {
            Slice input = null;
            if (!arrayBlock.isNull(position)) {
                input = elementType.getSlice(arrayBlock, position);
            }

            Boolean keep;
            try {
                keep = (Boolean) function.invokeExact(input);
            }
            catch (Throwable throwable) {
                throw Throwables.propagate(throwable);
            }
            if (TRUE.equals(keep)) {
                elementType.appendTo(arrayBlock, position, resultBuilder);
            }
        }
        return resultBuilder.build();
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Block.class)
    @SqlType("array(T)")
    public static Block filterBlock(@TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") MethodHandle function)
    {
        int positionCount = arrayBlock.getPositionCount();
        BlockBuilder resultBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), positionCount);
        for (int position = 0; position < positionCount; position++) {
            Block input = null;
            if (!arrayBlock.isNull(position)) {
                input = (Block) elementType.getObject(arrayBlock, position);
            }

            Boolean keep;
            try {
                keep = (Boolean) function.invokeExact(input);
            }
            catch (Throwable throwable) {
                throw Throwables.propagate(throwable);
            }
            if (TRUE.equals(keep)) {
                elementType.appendTo(arrayBlock, position, resultBuilder);
            }
        }
        return resultBuilder.build();
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = void.class)
    @SqlType("array(T)")
    public static Block filterVoid(@TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") MethodHandle function)
    {
        int positionCount = arrayBlock.getPositionCount();
        BlockBuilder resultBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), positionCount);
        for (int position = 0; position < positionCount; position++) {
            Boolean keep;
            try {
                keep = (Boolean) function.invokeExact(null);
            }
            catch (Throwable throwable) {
                throw Throwables.propagate(throwable);
            }
            if (TRUE.equals(keep)) {
                resultBuilder.appendNull();
            }
        }
        return resultBuilder.build();
    }
}
