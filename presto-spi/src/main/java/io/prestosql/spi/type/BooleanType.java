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
package io.prestosql.spi.type;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.block.ByteArrayBlockBuilder;
import io.prestosql.spi.block.PageBuilderStatus;
import io.prestosql.spi.connector.ConnectorSession;

import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;

public final class BooleanType
        extends AbstractType
        implements FixedWidthType
{
    public static final BooleanType BOOLEAN = new BooleanType();

    private BooleanType()
    {
        super(parseTypeSignature(StandardTypes.BOOLEAN), boolean.class);
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
                Math.min(expectedEntries, maxBlockSizeInBytes / Byte.BYTES));
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, Byte.BYTES);
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
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        return block.getByte(position, 0) != 0;
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        boolean leftValue = leftBlock.getByte(leftPosition, 0) != 0;
        boolean rightValue = rightBlock.getByte(rightPosition, 0) != 0;
        return leftValue == rightValue;
    }

    @Override
    public long hash(Block block, int position)
    {
        boolean value = block.getByte(position, 0) != 0;
        return value ? 1231 : 1237;
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        boolean leftValue = leftBlock.getByte(leftPosition, 0) != 0;
        boolean rightValue = rightBlock.getByte(rightPosition, 0) != 0;
        return Boolean.compare(leftValue, rightValue);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeByte(block.getByte(position, 0)).closeEntry();
        }
    }

    @Override
    public boolean getBoolean(Block block, int position)
    {
        return block.getByte(position, 0) != 0;
    }

    @Override
    public void writeBoolean(BlockBuilder blockBuilder, boolean value)
    {
        blockBuilder.writeByte(value ? 1 : 0).closeEntry();
    }

    @Override
    public boolean equals(Object other)
    {
        return other == BOOLEAN;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
