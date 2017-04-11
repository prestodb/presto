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
package com.facebook.presto.rcfile.text;

import com.facebook.presto.rcfile.ColumnData;
import com.facebook.presto.rcfile.EncodeOutput;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.util.Base64;

public class BinaryEncoding
        implements TextColumnEncoding
{
    private static final Base64.Decoder base64Decoder = Base64.getDecoder();
    private static final Base64.Encoder base64Encoder = Base64.getEncoder();

    private final Type type;
    private final Slice nullSequence;

    public BinaryEncoding(Type type, Slice nullSequence)
    {
        this.type = type;
        this.nullSequence = nullSequence;
    }

    @Override
    public void encodeColumn(Block block, SliceOutput output, EncodeOutput encodeOutput)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                output.writeBytes(nullSequence);
            }
            else {
                Slice slice = type.getSlice(block, position);
                byte[] data = slice.getBytes();
                slice = Slices.wrappedBuffer(base64Encoder.encode(data));
                output.writeBytes(slice);
            }
            encodeOutput.closeEntry();
        }
    }

    @Override
    public void encodeValueInto(int depth, Block block, int position, SliceOutput output)
    {
        Slice slice = type.getSlice(block, position);
        byte[] data = slice.getBytes();
        slice = Slices.wrappedBuffer(base64Encoder.encode(data));
        output.writeBytes(slice);
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
            if (nullSequence.equals(0, nullSequence.length(), slice, offset, length)) {
                builder.appendNull();
            }
            else {
                byte[] data = slice.getBytes(offset, length);
                type.writeSlice(builder, Slices.wrappedBuffer(base64Decoder.decode(data)));
            }
        }
        return builder.build();
    }

    @Override
    public void decodeValueInto(int depth, BlockBuilder builder, Slice slice, int offset, int length)
    {
        byte[] data = slice.getBytes(offset, length);
        type.writeSlice(builder, Slices.wrappedBuffer(base64Decoder.decode(data)));
    }
}
