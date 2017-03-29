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
package com.facebook.presto.hive.coercions;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public abstract class AbstractCoercer<F extends Type, T extends Type>
        implements Function<Block, Block>
{
    protected final F fromType;
    protected final T toType;

    protected AbstractCoercer(F fromType, T toType)
    {
        this.fromType = requireNonNull(fromType, "fromType is null");
        this.toType = requireNonNull(toType, "toType is null");
    }

    @Override
    public Block apply(Block block)
    {
        BlockBuilder blockBuilder = toType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount());
        if (fromType.getJavaType() == long.class) {
            applyLong(blockBuilder, block);
        }
        else if (fromType.getJavaType() == Slice.class) {
            applySlice(blockBuilder, block);
        }
        else if (fromType.getJavaType() == double.class) {
            applyDouble(blockBuilder, block);
        }
        else if (fromType.getJavaType() == boolean.class) {
            applyBoolean(blockBuilder, block);
        }
        else {
            throw new IllegalArgumentException("Unsupported container type " + fromType.getJavaType());
        }
        return blockBuilder.build();
    }

    private void applyLong(BlockBuilder blockBuilder, Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                blockBuilder.appendNull();
                continue;
            }
            appendCoercedLong(blockBuilder, fromType.getLong(block, i));
        }
    }

    protected void appendCoercedLong(BlockBuilder blockBuilder, long aLong)
    {
        throw new UnsupportedOperationException();
    }

    private void applySlice(BlockBuilder blockBuilder, Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                blockBuilder.appendNull();
                continue;
            }
            appendCoercedSlice(blockBuilder, fromType.getSlice(block, i));
        }
    }

    protected void appendCoercedSlice(BlockBuilder blockBuilder, Slice slice)
    {
        throw new UnsupportedOperationException();
    }

    private void applyDouble(BlockBuilder blockBuilder, Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                blockBuilder.appendNull();
                continue;
            }
            appendCoercedDouble(blockBuilder, fromType.getDouble(block, i));
        }
    }

    protected void appendCoercedDouble(BlockBuilder blockBuilder, double aDouble)
    {
        throw new UnsupportedOperationException();
    }

    private void applyBoolean(BlockBuilder blockBuilder, Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                blockBuilder.appendNull();
                continue;
            }
            appendCoercedBoolean(blockBuilder, fromType.getBoolean(block, i));
        }
    }

    protected void appendCoercedBoolean(BlockBuilder blockBuilder, boolean aBoolean)
    {
        throw new UnsupportedOperationException();
    }
}
