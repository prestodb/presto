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
package com.facebook.presto.spi.block;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.spi.block.EncoderUtil.decodeNullBits;
import static com.facebook.presto.spi.block.EncoderUtil.encodeNullsAsBits;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;


public class LongArrayBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "LONG_ARRAY";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        int positionCount = block.getPositionCount();
        sliceOutput.appendInt(positionCount);

        encodeNullsAsBits(sliceOutput, block);

        for (int position = 0; position < positionCount; position++) {
            if (!block.isNull(position)) {
                sliceOutput.writeLong(block.getLong(position, 0));
            }
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        long[] values = new long[positionCount];
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                values[position] = sliceInput.readLong();
            }
        }

        return new LongArrayBlock(0, positionCount, valueIsNull, values);
    }

    public Block readBlockReusing(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput, BlockDecoder toReuse)
    {
        int positionCount = sliceInput.readInt();
        long[] values = toReuse.getLongs();
        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount, toReuse != null ? toReuse.getValueIsNull() : null).orElse(null);
        if (values == null || values.length < positionCount) {
            values = new long[positionCount];
        }
        if (sliceInput instanceof ConcatenatedByteArrayInputStream) {
            ConcatenatedByteArrayInputStream input = (ConcatenatedByteArrayInputStream) sliceInput;
            int position = 0;
            newBuffer:
            while (position < positionCount) {
                int available = input.contiguousAvailable();
                int toRead = Math.min(available / SIZE_OF_LONG, positionCount - position);
                byte[] buffer = input.getBuffer();
                int offset = input.getOffsetInBuffer();
                if (valueIsNull == null) {
                    if (toRead == 0) {
                        values[position++] = input.readLong();
                    }
                    else {
                        ByteArrayUtils.copyToLongs(buffer, offset, values, position, toRead);
                        input.skip(toRead * SIZE_OF_LONG);
                        position += toRead;
                    }
                }
                else {
                    int bytesRead = 0;
                    while (position < positionCount) {
                        if (valueIsNull[position]) {
                            position++;
                            continue;
                        }
                        else {
                            if (toRead > 0) {
                                values[position++] = ByteArrayUtils.getLong(buffer, offset + bytesRead);
                                toRead--;
                                bytesRead += SIZE_OF_LONG;
                            }
                            else {
                                input.skip(bytesRead);
                                values[position++] = input.readLong();
                                bytesRead = 0;
                                continue newBuffer;
                            }
                        }
                    }
                    input.skip(bytesRead);
                    break;
                }
            }
        }
        else {
            for (int position = 0; position < positionCount; position++) {
                if (valueIsNull == null || !valueIsNull[position]) {
                    values[position] = sliceInput.readLong();
                }
            }
        }

        return new LongArrayBlock(0, positionCount, valueIsNull, values);
    }

    @Override
    public int reserveBytesInBuffer(BlockDecoder contents, int numValues, int startInBuffer, EncodingState state)
    {
        //  Reserves space for serialized 'rows' non-null longs
        // including headers. 5 for vallue count and null indicator, 4
        // for name character count + length of the name string.
        int size = 8 * numValues + 5 + 4 + NAME.length();
        state.startInBuffer = startInBuffer;
        state.bytesInBuffer = size;
        state.maxValues = numValues;
        state.encodingName = NAME;
        return startInBuffer + size;
    }

    public static boolean useGather = true;

    @Override
    public void addValues(BlockDecoder contents, int[] rows, int firstRow, int numRows, EncodingState state)
    {
        long[] longs = contents.longs;
        int[] map = contents.isIdentityMap ? null : contents.rowNumberMap;
        int longsOffset = state.valueOffset + 5 + state.numValues * SIZE_OF_LONG + (int) state.topLevelBuffer.getAddress() - ARRAY_BYTE_BASE_OFFSET;
        byte[] target = (byte[]) state.topLevelBuffer.getBase();
        if (!useGather) {
            if (map == null) {
                for (int i = 0; i < numRows; i++) {
                    state.topLevelBuffer.setLong(longsOffset + i *SIZE_OF_LONG, longs[rows[i + firstRow]]);
                }
            }
            else {
                for (int i = 0; i < numRows; i++) {
                    state.topLevelBuffer.setLong(longsOffset + i *SIZE_OF_LONG, longs[map[rows[i + firstRow]]]);
                }
            }
        }
        else {
            ByteArrayUtils.gather(longs, rows, map, firstRow, target, longsOffset, numRows);
        }
        state.numValues += numRows;
    }

    @Override
    public int prepareFinish(EncodingState state, int newStartInBuffer)
    {
        state.newStartInBuffer = newStartInBuffer;
        return finalSize(state);
    }
    int finalSize(EncodingState state)
    {
        return         8 * state.numValues + (state.valueOffset - state.startInBuffer) + 5;
    }

    @Override
    public void finish(EncodingState state, Slice buffer)
    {
        state.topLevelBuffer.setInt(state.valueOffset, state.numValues);
        state.topLevelBuffer.setByte(state.valueOffset + 4, 0);
        if (buffer.getBase() == state.topLevelBuffer.getBase() && !state.anyNulls && state.startInBuffer == state.newStartInBuffer) {
            return;
        }
        int size = finalSize(state);
        System.arraycopy((byte[])state.topLevelBuffer.getBase(),
                         state.startInBuffer,
                         (byte[])buffer.getBase(),
                         state.newStartInBuffer,
                         size);
    }

    @Override
    public boolean supportsReadBlockReusing()
    {
        return true;
    }
}
