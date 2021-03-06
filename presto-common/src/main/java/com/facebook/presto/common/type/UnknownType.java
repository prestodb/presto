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
package com.facebook.presto.common.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.common.block.ByteArrayBlockBuilder;
import com.facebook.presto.common.block.PageBuilderStatus;
import com.facebook.presto.common.function.SqlFunctionProperties;

public final class UnknownType
        extends AbstractType
        implements FixedWidthType
{
    public static final UnknownType UNKNOWN = new UnknownType();
    public static final String NAME = "unknown";

    private UnknownType()
    {
        // We never access the native container for UNKNOWN because its null check is always true.
        // The actual native container type does not matter here.
        // We choose boolean to represent UNKNOWN because it's the smallest primitive type.
        super(new TypeSignature(NAME), boolean.class);
    }

    private static void checkArgument(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    @Override
    public int getFixedSize()
    {
        return Byte.BYTES;
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        int maxBlockSizeInBytes;
        if (blockBuilderStatus == null) {
            maxBlockSizeInBytes = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
        }
        else {
            maxBlockSizeInBytes = blockBuilderStatus.getMaxPageSizeInBytes();
        }
        return new ByteArrayBlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / getFixedSize()));
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, getFixedSize());
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new ByteArrayBlockBuilder(null, positionCount);
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }

    @Override
    public long hash(Block block, int position)
    {
        // Check that the position is valid
        checkArgument(block.isNull(position), "Expected NULL value for UnknownType");
        return 0;
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        // Check that the position is valid
        checkArgument(leftBlock.isNull(leftPosition), "Expected NULL value for UnknownType");
        checkArgument(rightBlock.isNull(rightPosition), "Expected NULL value for UnknownType");
        return true;
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        // Check that the position is valid
        checkArgument(leftBlock.isNull(leftPosition), "Expected NULL value for UnknownType");
        checkArgument(rightBlock.isNull(rightPosition), "Expected NULL value for UnknownType");
        return 0;
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        // call is null in case position is out of bounds
        checkArgument(block.isNull(position), "Expected NULL value for UnknownType");
        return null;
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        blockBuilder.appendNull();
    }

    @Override
    public boolean getBoolean(Block block, int position)
    {
        // Ideally, this function should never be invoked for unknown type.
        // However, some logic rely on having a default value before the null check.
        if (!block.isNull(position)) {
            throw new UnsupportedOperationException();
        }

        return false;
    }

    @Deprecated
    @Override
    public void writeBoolean(BlockBuilder blockBuilder, boolean value)
    {
        // Ideally, this function should never be invoked for unknown type.
        // However, some logic (e.g. AbstractMinMaxBy) rely on writing a default value before the null check.
        if (value) {
            throw new UnsupportedOperationException();
        }

        blockBuilder.appendNull();
    }
}
