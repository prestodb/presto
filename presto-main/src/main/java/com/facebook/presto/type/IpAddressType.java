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
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;

public class IpAddressType
        extends AbstractFixedWidthType
{
    public static final IpAddressType IPADDRESS = new IpAddressType();

    private IpAddressType()
    {
        super(parseTypeSignature(StandardTypes.IPADDRESS), Slice.class, 16);
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
        return compareTo(leftBlock, leftPosition, rightBlock, rightPosition) == 0;
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return leftBlock.compareTo(leftPosition, 0, getFixedSize(), rightBlock, rightPosition, 0, getFixedSize());
    }

    @Override
    public long hash(Block block, int position)
    {
        return block.hash(position, 0, getFixedSize());
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        try {
            return InetAddresses.toAddrString(InetAddress.getByAddress(block.getSlice(position, 0, getFixedSize()).getBytes()));
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
            block.writeBytesTo(position, 0, getFixedSize(), blockBuilder);
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
        blockBuilder.writeBytes(value, offset, length).closeEntry();
    }
}
