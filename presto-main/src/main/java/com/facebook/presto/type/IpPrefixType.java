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
import com.facebook.presto.common.block.PageBuilderStatus;
import com.facebook.presto.common.block.VariableWidthBlockBuilder;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.AbstractType;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.StandardTypes;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;

public class IpPrefixType
        extends AbstractType
        implements FixedWidthType
{
    public static final IpPrefixType IPPREFIX = new IpPrefixType();

    private IpPrefixType()
    {
        super(parseTypeSignature(StandardTypes.IPPREFIX), Slice.class);
    }

    @Override
    public int getFixedSize()
    {
        return Long.BYTES * 2 + 1;
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
        return new VariableWidthBlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / getFixedSize()),
                Math.min(expectedEntries * getFixedSize(), maxBlockSizeInBytes));
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, getFixedSize());
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return createBlockBuilder(null, positionCount);
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
        return leftBlock.equals(leftPosition, 0, rightBlock, rightPosition, 0, getFixedSize());
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return leftBlock.compareTo(leftPosition, 0, getFixedSize(), rightBlock, rightPosition, 0, getFixedSize());
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
            String addrString = InetAddresses.toAddrString(InetAddress.getByAddress(getSlice(block, position).getBytes(0, 2 * Long.BYTES)));
            String prefixString = Integer.toString(getSlice(block, position).getByte(2 * Long.BYTES) & 0xff);
            return addrString + "/" + prefixString;
        }
        catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeBytes(block.getSlice(position, 0, getFixedSize()), 0, getFixedSize());
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
        if (length != getFixedSize()) {
            throw new IllegalStateException("Expected entry size to be exactly " + getFixedSize() + " but was " + length);
        }
        blockBuilder.writeBytes(value, 0, length);
        blockBuilder.closeEntry();
    }

    @Override
    public final Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, getFixedSize());
    }
}
