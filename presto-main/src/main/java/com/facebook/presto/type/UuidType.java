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
package com.facebook.presto.type;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.AbstractFixedWidthType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.ByteBuffer;
import java.util.UUID;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class UuidType
        extends AbstractFixedWidthType
{
    public static final UuidType UUID_TYPE = new UuidType();
    public static final String NAME = "uuid";

    private static final int entrySize = 2 * SIZE_OF_LONG;

    private UuidType()
    {
        super(parseTypeSignature(NAME), Slice.class, entrySize);
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        return getObject(block, position);
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
    public Object getObject(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        return new UUID(block.getLong(position, 0), block.getLong(position, SIZE_OF_LONG));
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeLong(block.getLong(position, 0));
            blockBuilder.writeLong(block.getLong(position, SIZE_OF_LONG)).closeEntry();
        }
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        UUID leftValue = new UUID(leftBlock.getLong(leftPosition, 0), leftBlock.getLong(leftPosition, SIZE_OF_LONG));
        UUID rightValue = new UUID(rightBlock.getLong(rightPosition, 0), rightBlock.getLong(rightPosition, SIZE_OF_LONG));
        return leftValue.equals(rightValue);
    }

    @Override
    public int hash(Block block, int position)
    {
        return new UUID(block.getLong(position, 0), block.getLong(position, SIZE_OF_LONG)).hashCode();
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        checkArgument(value instanceof UUID);
        UUID uuid = (UUID) value;

        blockBuilder.writeLong(uuid.getMostSignificantBits());
        blockBuilder.writeLong(uuid.getLeastSignificantBits());

        blockBuilder.closeEntry();
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        UUID leftValue = new UUID(leftBlock.getLong(leftPosition, 0), leftBlock.getLong(leftPosition, SIZE_OF_LONG));
        UUID rightValue = new UUID(rightBlock.getLong(rightPosition, 0), rightBlock.getLong(rightPosition, SIZE_OF_LONG));
        return leftValue.compareTo(rightValue);
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        blockBuilder.writeBytes(value, offset, length).closeEntry();
    }

    public static UUID uuidFromSlice(Slice slice)
    {
        ByteBuffer buffer = ByteBuffer.wrap(slice.getBytes());
        long msb = buffer.getLong();
        long lsb = buffer.getLong();
        return new UUID(msb, lsb);
    }

    public static Slice uuidToSlice(UUID uuid)
    {
        return Slices.wrappedBuffer(ByteBuffer.allocate(16)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array());
    }
}
