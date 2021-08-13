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
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.function.TypeParameterSpecialization;
import io.airlift.slice.Slice;

import static java.lang.Boolean.TRUE;

@Description("return array containing elements that match the given predicate")
@ScalarFunction(value = "filter", deterministic = false)
public final class ArrayFilterFunction
{
    private ArrayFilterFunction() {}

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = long.class)
    @SqlType("array(T)")
    public static Block filterLong(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") LongToBooleanFunction function)
    {
        int positionCount = arrayBlock.getPositionCount();
        int position = 0;
        BlockBuilder resultBuilder;

        if (arrayBlock.mayHaveNull()) {
            while (position < positionCount &&
                    TRUE.equals(function.apply(arrayBlock.isNull(position) ?
                                               null :
                                               elementType.getLong(arrayBlock, position)))) {
                position++;
            }

            if (position == positionCount) {
                // Nothing fitered out. So just return the original.
                return arrayBlock;
            }

            resultBuilder = elementType.createBlockBuilder(null, positionCount);
            for (int i = 0; i < position; i++) {
                elementType.appendTo(arrayBlock, i, resultBuilder);
            }

            for (position++; position < positionCount; position++) {
                if (TRUE.equals(function.apply(arrayBlock.isNull(position) ?
                                               null :
                                               elementType.getLong(arrayBlock, position)))) {
                    elementType.appendTo(arrayBlock, position, resultBuilder);
                }
            }
        }
        else {
            while (position < positionCount && TRUE.equals(function.apply(elementType.getLong(arrayBlock, position)))) {
                position++;
            }

            if (position == positionCount) {
                // Nothing filtered out. So just return the original.
                return arrayBlock;
            }

            resultBuilder = elementType.createBlockBuilder(null, positionCount);
            for (int i = 0; i < position; i++) {
                elementType.appendTo(arrayBlock, i, resultBuilder);
            }
            for (position++; position < positionCount; position++) {
                if (TRUE.equals(function.apply(elementType.getLong(arrayBlock, position)))) {
                    elementType.appendTo(arrayBlock, position, resultBuilder);
                }
            }
        }

        return resultBuilder.build();
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = double.class)
    @SqlType("array(T)")
    public static Block filterDouble(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") DoubleToBooleanFunction function)
    {
        int positionCount = arrayBlock.getPositionCount();
        int position = 0;
        BlockBuilder resultBuilder;

        if (arrayBlock.mayHaveNull()) {
            while (position < positionCount &&
                    TRUE.equals(function.apply(arrayBlock.isNull(position) ?
                                               null :
                                               elementType.getDouble(arrayBlock, position)))) {
                position++;
            }

            if (position == positionCount) {
                // Nothing fitered out. So just return the original.
                return arrayBlock;
            }

            resultBuilder = elementType.createBlockBuilder(null, positionCount);
            for (int i = 0; i < position; i++) {
                elementType.appendTo(arrayBlock, i, resultBuilder);
            }
            for (position++; position < positionCount; position++) {
                if (TRUE.equals(function.apply(arrayBlock.isNull(position) ?
                                               null :
                                               elementType.getDouble(arrayBlock, position)))) {
                    elementType.appendTo(arrayBlock, position, resultBuilder);
                }
            }
        }
        else {
            while (position < positionCount && TRUE.equals(function.apply(elementType.getDouble(arrayBlock, position)))) {
                position++;
            }

            if (position == positionCount) {
                // Nothing filtered out. So just return the original.
                return arrayBlock;
            }

            resultBuilder = elementType.createBlockBuilder(null, positionCount);
            for (int i = 0; i < position; i++) {
                elementType.appendTo(arrayBlock, i, resultBuilder);
            }
            for (position++; position < positionCount; position++) {
                if (TRUE.equals(function.apply(elementType.getDouble(arrayBlock, position)))) {
                    elementType.appendTo(arrayBlock, position, resultBuilder);
                }
            }
        }

        return resultBuilder.build();
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = boolean.class)
    @SqlType("array(T)")
    public static Block filterBoolean(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") BooleanToBooleanFunction function)
    {
        int positionCount = arrayBlock.getPositionCount();
        int position = 0;
        BlockBuilder resultBuilder;

        if (arrayBlock.mayHaveNull()) {
            while (position < positionCount &&
                    TRUE.equals(function.apply(arrayBlock.isNull(position) ?
                                               null :
                                               elementType.getBoolean(arrayBlock, position)))) {
                position++;
            }

            if (position == positionCount) {
                // Nothing fitered out. So just return the original.
                return arrayBlock;
            }

            resultBuilder = elementType.createBlockBuilder(null, positionCount);
            for (int i = 0; i < position; i++) {
                elementType.appendTo(arrayBlock, i, resultBuilder);
            }
            for (position++; position < positionCount; position++) {
                if (TRUE.equals(function.apply(arrayBlock.isNull(position) ?
                                               null :
                                               elementType.getBoolean(arrayBlock, position)))) {
                    elementType.appendTo(arrayBlock, position, resultBuilder);
                }
            }
        }
        else {
            while (position < positionCount && TRUE.equals(function.apply(elementType.getBoolean(arrayBlock, position)))) {
                position++;
            }

            if (position == positionCount) {
                // Nothing filtered out. So just return the original.
                return arrayBlock;
            }

            resultBuilder = elementType.createBlockBuilder(null, positionCount);
            for (int i = 0; i < position; i++) {
                elementType.appendTo(arrayBlock, i, resultBuilder);
            }
            for (position++; position < positionCount; position++) {
                if (TRUE.equals(function.apply(elementType.getBoolean(arrayBlock, position)))) {
                    elementType.appendTo(arrayBlock, position, resultBuilder);
                }
            }
        }

        return resultBuilder.build();
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Slice.class)
    @SqlType("array(T)")
    public static Block filterSlice(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") SliceToBooleanFunction function)
    {
        int positionCount = arrayBlock.getPositionCount();
        int position = 0;
        BlockBuilder resultBuilder;

        if (arrayBlock.mayHaveNull()) {
            while (position < positionCount &&
                    TRUE.equals(function.apply(arrayBlock.isNull(position) ?
                                               null :
                                               elementType.getSlice(arrayBlock, position)))) {
                position++;
            }

            if (position == positionCount) {
                // Nothing fitered out. So just return the original.
                return arrayBlock;
            }

            resultBuilder = elementType.createBlockBuilder(null, positionCount);
            for (int i = 0; i < position; i++) {
                elementType.appendTo(arrayBlock, i, resultBuilder);
            }
            for (position++; position < positionCount; position++) {
                if (TRUE.equals(function.apply(arrayBlock.isNull(position) ?
                                               null :
                                               elementType.getSlice(arrayBlock, position)))) {
                    elementType.appendTo(arrayBlock, position, resultBuilder);
                }
            }
        }
        else {
            while (position < positionCount && TRUE.equals(function.apply(elementType.getSlice(arrayBlock, position)))) {
                position++;
            }

            if (position == positionCount) {
                // Nothing filtered out. So just return the original.
                return arrayBlock;
            }

            resultBuilder = elementType.createBlockBuilder(null, positionCount);
            for (int i = 0; i < position; i++) {
                elementType.appendTo(arrayBlock, i, resultBuilder);
            }
            for (position++; position < positionCount; position++) {
                if (TRUE.equals(function.apply(elementType.getSlice(arrayBlock, position)))) {
                    elementType.appendTo(arrayBlock, position, resultBuilder);
                }
            }
        }

        return resultBuilder.build();
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Block.class)
    @SqlType("array(T)")
    public static Block filterBlock(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") BlockToBooleanFunction function)
    {
        int positionCount = arrayBlock.getPositionCount();
        int position = 0;
        BlockBuilder resultBuilder;

        if (arrayBlock.mayHaveNull()) {
            while (position < positionCount &&
                    TRUE.equals(function.apply(arrayBlock.isNull(position) ?
                                               null :
                                               (Block) elementType.getObject(arrayBlock, position)))) {
                position++;
            }

            if (position == positionCount) {
                // Nothing fitered out. So just return the original.
                return arrayBlock;
            }

            resultBuilder = elementType.createBlockBuilder(null, positionCount);
            for (int i = 0; i < position; i++) {
                elementType.appendTo(arrayBlock, i, resultBuilder);
            }
            for (position++; position < positionCount; position++) {
                if (TRUE.equals(function.apply(arrayBlock.isNull(position) ?
                                               null :
                                               (Block) elementType.getObject(arrayBlock, position)))) {
                    elementType.appendTo(arrayBlock, position, resultBuilder);
                }
            }
        }
        else {
            while (position < positionCount && TRUE.equals(function.apply((Block) elementType.getObject(arrayBlock, position)))) {
                position++;
            }

            if (position == positionCount) {
                // Nothing filtered out. So just return the original.
                return arrayBlock;
            }

            resultBuilder = elementType.createBlockBuilder(null, positionCount);
            for (int i = 0; i < position; i++) {
                elementType.appendTo(arrayBlock, i, resultBuilder);
            }
            for (position++; position < positionCount; position++) {
                if (TRUE.equals(function.apply((Block) elementType.getObject(arrayBlock, position)))) {
                    elementType.appendTo(arrayBlock, position, resultBuilder);
                }
            }
        }

        return resultBuilder.build();
    }
}
