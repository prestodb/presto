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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.common.block.Int128ArrayBlockBuilder;
import com.facebook.presto.common.block.PageBuilderStatus;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.AbstractType;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.StandardTypes;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.facebook.presto.common.block.Int128ArrayBlock.INT128_BYTES;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.lang.Long.reverseBytes;

public class IpAddressType
        extends AbstractType
        implements FixedWidthType
{
    public static final IpAddressType IPADDRESS = new IpAddressType();

    private IpAddressType()
    {
        super(parseTypeSignature(StandardTypes.IPADDRESS), Slice.class);
    }

    @Override
    public int getFixedSize()
    {
        return INT128_BYTES;
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
        return new Int128ArrayBlockBuilder(
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
        return new Int128ArrayBlockBuilder(null, positionCount);
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
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return leftBlock.getLong(leftPosition, SIZE_OF_LONG) == rightBlock.getLong(rightPosition, SIZE_OF_LONG) &&
                leftBlock.getLong(leftPosition, 0) == rightBlock.getLong(rightPosition, 0);
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        int compare = Long.compareUnsigned(reverseBytes(leftBlock.getLong(leftPosition, 0)), reverseBytes(rightBlock.getLong(rightPosition, 0)));
        if (compare != 0) {
            return compare;
        }
        return Long.compareUnsigned(reverseBytes(leftBlock.getLong(leftPosition, SIZE_OF_LONG)), reverseBytes(rightBlock.getLong(rightPosition, SIZE_OF_LONG)));
    }

    @Override
    public long hash(Block block, int position)
    {
        return XxHash64.hash(getSlice(block, position));
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        try {
            return InetAddresses.toAddrString(InetAddress.getByAddress(getSlice(block, position).getBytes()));
        }
        catch (UnknownHostException e) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeLong(block.getLong(position, 0));
            blockBuilder.writeLong(block.getLong(position, SIZE_OF_LONG));
            blockBuilder.closeEntry();
        }
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        if (length != INT128_BYTES) {
            throw new IllegalStateException("Expected entry size to be exactly " + INT128_BYTES + " but was " + length);
        }
        blockBuilder.writeLong(value.getLong(offset));
        blockBuilder.writeLong(value.getLong(offset + SIZE_OF_LONG));
        blockBuilder.closeEntry();
    }

    @Override
    public final Slice getSlice(Block block, int position)
    {
        return Slices.wrappedLongArray(
                block.getLong(position, 0),
                block.getLong(position, SIZE_OF_LONG));
    }
}
