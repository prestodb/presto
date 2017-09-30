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
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.StandardTypes.BIT;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Strings.padStart;
import static java.lang.Integer.toBinaryString;

public class BitType
        extends AbstractFixedWidthType
{
    public static final int MAX_BITS = 0x4000000; // 8MB

    private static final String[] BINARY_STRINGS = binaryStrings();

    public static BitType bitType(int bits)
    {
        checkCondition(bits > 0, NOT_SUPPORTED, "Bit length must be at least 1");
        checkCondition(bits <= MAX_BITS, NOT_SUPPORTED, "Bit length must be no more than " + MAX_BITS);
        return new BitType(bits);
    }

    private final int bits;

    private BitType(int bits)
    {
        super(new TypeSignature(BIT, TypeSignatureParameter.of(bits)), Slice.class, byteCount(bits));
        this.bits = bits;
    }

    public int getLength()
    {
        return bits;
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
    public long hash(Block block, int position)
    {
        return block.hash(position, 0, getFixedSize());
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return leftBlock.compareTo(leftPosition, 0, getFixedSize(), rightBlock, rightPosition, 0, getFixedSize());
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        Slice slice = block.getSlice(position, 0, getFixedSize());

        StringBuilder sb = new StringBuilder();

        int prefix = bits % 8;
        int start = 0;
        if (prefix > 0) {
            start = 1;
            sb.append(BINARY_STRINGS[slice.getUnsignedByte(0)].substring(8 - prefix));
        }

        for (int i = start; i < slice.length(); i++) {
            sb.append(BINARY_STRINGS[slice.getUnsignedByte(i)]);
        }

        return sb.toString();
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

    public static int byteCount(int bits)
    {
        return ((bits - 1) / 8) + 1;
    }

    public static String[] binaryStrings()
    {
        String[] values = new String[256];
        for (int i = 0; i < 256; i++) {
            values[i] = padStart(toBinaryString(i), 8, '0');
        }
        return values;
    }
}
