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
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import static com.facebook.presto.type.TypeUtils.buildStructuralSlice;
import static com.facebook.presto.type.TypeUtils.readArrayBlock;

public final class ArrayConcatUtils
{
    private ArrayConcatUtils() {}

    public static Slice concat(Type elementType, Slice left, Slice right)
    {
        Block leftBlock = readArrayBlock(elementType, left);
        Block rightBlock = readArrayBlock(elementType, right);
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), leftBlock.getPositionCount() + rightBlock.getPositionCount());
        for (int i = 0; i < leftBlock.getPositionCount(); i++) {
            elementType.appendTo(leftBlock, i, blockBuilder);
        }
        for (int i = 0; i < rightBlock.getPositionCount(); i++) {
            elementType.appendTo(rightBlock, i, blockBuilder);
        }
        return buildStructuralSlice(blockBuilder);
    }

    public static Slice appendElement(Type elementType, Slice in, long value)
    {
        Block block = readArrayBlock(elementType, in);
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount() + 1);
        for (int i = 0; i < block.getPositionCount(); i++) {
            elementType.appendTo(block, i, blockBuilder);
        }

        elementType.writeLong(blockBuilder, value);

        return buildStructuralSlice(blockBuilder);
    }

    public static Slice appendElement(Type elementType, Slice in, boolean value)
    {
        Block block = readArrayBlock(elementType, in);
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount() + 1);
        for (int i = 0; i < block.getPositionCount(); i++) {
            elementType.appendTo(block, i, blockBuilder);
        }

        elementType.writeBoolean(blockBuilder, value);

        return buildStructuralSlice(blockBuilder);
    }

    public static Slice appendElement(Type elementType, Slice in, double value)
    {
        Block block = readArrayBlock(elementType, in);
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount() + 1);
        for (int i = 0; i < block.getPositionCount(); i++) {
            elementType.appendTo(block, i, blockBuilder);
        }

        elementType.writeDouble(blockBuilder, value);

        return buildStructuralSlice(blockBuilder);
    }

    public static Slice appendElement(Type elementType, Slice in, Slice value)
    {
        Block block = readArrayBlock(elementType, in);
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount() + 1);
        for (int i = 0; i < block.getPositionCount(); i++) {
            elementType.appendTo(block, i, blockBuilder);
        }

        elementType.writeSlice(blockBuilder, value);

        return buildStructuralSlice(blockBuilder);
    }

    public static Slice prependElement(Type elementType, Slice value, Slice in)
    {
        Block block = readArrayBlock(elementType, in);
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount() + 1);

        elementType.writeSlice(blockBuilder, value);
        for (int i = 0; i < block.getPositionCount(); i++) {
            elementType.appendTo(block, i, blockBuilder);
        }

        return buildStructuralSlice(blockBuilder);
    }

    public static Slice prependElement(Type elementType, long value, Slice in)
    {
        Block block = readArrayBlock(elementType, in);
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount() + 1);

        elementType.writeLong(blockBuilder, value);
        for (int i = 0; i < block.getPositionCount(); i++) {
            elementType.appendTo(block, i, blockBuilder);
        }

        return buildStructuralSlice(blockBuilder);
    }

    public static Slice prependElement(Type elementType, boolean value, Slice in)
    {
        Block block = readArrayBlock(elementType, in);
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount() + 1);

        elementType.writeBoolean(blockBuilder, value);
        for (int i = 0; i < block.getPositionCount(); i++) {
            elementType.appendTo(block, i, blockBuilder);
        }

        return buildStructuralSlice(blockBuilder);
    }

    public static Slice prependElement(Type elementType, double value, Slice in)
    {
        Block block = readArrayBlock(elementType, in);
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount() + 1);

        elementType.writeDouble(blockBuilder, value);
        for (int i = 0; i < block.getPositionCount(); i++) {
            elementType.appendTo(block, i, blockBuilder);
        }

        return buildStructuralSlice(blockBuilder);
    }
}
