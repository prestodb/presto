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
package com.facebook.presto.rcfile.binary;

import com.facebook.presto.rcfile.ColumnData;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.math.BigInteger;

import static com.facebook.presto.rcfile.RcFileDecoderUtils.decodeVIntSize;
import static com.facebook.presto.rcfile.RcFileDecoderUtils.readVInt;
import static com.facebook.presto.spi.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.facebook.presto.spi.type.Decimals.rescale;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;

public class DecimalEncoding
        implements BinaryColumnEncoding
{
    private final DecimalType type;
    private final byte[] resultBytes = new byte[16];
    private final Slice resultSlice = Slices.wrappedBuffer(resultBytes);

    public DecimalEncoding(Type type)
    {
        this.type = (DecimalType) type;
    }

    @Override
    public Block decodeColumn(ColumnData columnData)
    {
        int size = columnData.rowCount();
        BlockBuilder builder = type.createBlockBuilder(new BlockBuilderStatus(), size);

        Slice slice = columnData.getSlice();
        for (int i = 0; i < size; i++) {
            int offset = columnData.getOffset(i);
            int length = columnData.getLength(i);
            if (length == 0) {
                builder.appendNull();
            }
            else if (isShortDecimal(type)) {
                type.writeLong(builder, parseLong(slice, offset));
            }
            else {
                type.writeSlice(builder, parseSlice(slice, offset));
            }
        }
        return builder.build();
    }

    @Override
    public int getValueOffset(Slice slice, int offset)
    {
        return 0;
    }

    @Override
    public int getValueLength(Slice slice, int offset)
    {
        // first vint is scale
        int scaleLength = decodeVIntSize(slice, offset);

        // second vint is data length
        int dataLengthLength = decodeVIntSize(slice, offset + scaleLength);
        int dataLength = toIntExact(readVInt(slice, offset + scaleLength));

        return scaleLength + dataLengthLength + dataLength;
    }

    @Override
    public void decodeValueInto(BlockBuilder builder, Slice slice, int offset, int length)
    {
        if (isShortDecimal(type)) {
            type.writeLong(builder, parseLong(slice, offset));
        }
        else {
            type.writeSlice(builder, parseSlice(slice, offset));
        }
    }

    private long parseLong(Slice slice, int offset)
    {
        // first vint is scale
        int scale = toIntExact(readVInt(slice, offset));
        offset += decodeVIntSize(slice, offset);

        // second vint is length
        int length = toIntExact(readVInt(slice, offset));
        offset += decodeVIntSize(slice, offset);

        checkState(length <= 8);

        // reset the results buffer
        if (slice.getByte(offset) >= 0) {
            resultSlice.setLong(0, 0L);
        }
        else {
            resultSlice.setLong(0, 0xFFFF_FFFF_FFFF_FFFFL);
        }

        resultSlice.setBytes(8 - length, slice, offset, length);

        long value = Long.reverseBytes(resultSlice.getLong(0));
        if (scale != type.getScale()) {
            return rescale(value, scale, type.getScale());
        }

        return value;
    }

    private Slice parseSlice(Slice slice, int offset)
    {
        // first vint is scale, which is ignored
        int scale = toIntExact(readVInt(slice, offset));
        offset += decodeVIntSize(slice, offset);

        // second vint is length
        int length = toIntExact(readVInt(slice, offset));
        offset += decodeVIntSize(slice, offset);

        checkState(length <= 16);

        // reset the results buffer
        if (slice.getByte(offset) >= 0) {
            resultSlice.setLong(0, 0L);
            resultSlice.setLong(8, 0L);
        }
        else {
            resultSlice.setLong(0, 0xFFFF_FFFF_FFFF_FFFFL);
            resultSlice.setLong(8, 0xFFFF_FFFF_FFFF_FFFFL);
        }

        resultSlice.setBytes(16 - length, slice, offset, length);

        if (scale != type.getScale()) {
            return encodeUnscaledValue(rescale(new BigInteger(resultBytes), scale, type.getScale()));
        }
        return resultSlice;
    }
}
