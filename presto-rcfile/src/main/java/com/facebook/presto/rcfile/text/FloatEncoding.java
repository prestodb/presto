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
import com.facebook.presto.rcfile.RcFileCorruptionException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

public class FloatEncoding
        implements TextColumnEncoding
{
    private final Type type;
    private final Slice nullSequence;

    public FloatEncoding(Type type, Slice nullSequence)
    {
        this.type = type;
        this.nullSequence = nullSequence;
    }

    @Override
    public Block decodeColumn(ColumnData columnData)
            throws RcFileCorruptionException
    {
        int size = columnData.rowCount();
        BlockBuilder builder = type.createBlockBuilder(new BlockBuilderStatus(), size);

        Slice slice = columnData.getSlice();
        for (int i = 0; i < size; i++) {
            int offset = columnData.getOffset(i);
            int length = columnData.getLength(i);
            if (length == 0 || nullSequence.equals(0, nullSequence.length(), slice, offset, length)) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, Float.floatToIntBits(parseFloat(slice, offset, length)));
            }
        }
        return builder.build();
    }

    @Override
    public void decodeValueInto(int depth, BlockBuilder builder, Slice slice, int offset, int length)
            throws RcFileCorruptionException
    {
        type.writeLong(builder, Float.floatToIntBits(parseFloat(slice, offset, length)));
    }

    private static float parseFloat(Slice slice, int start, int length)
            throws RcFileCorruptionException
    {
        try {
            return Float.parseFloat(slice.toStringAscii(start, length));
        }
        catch (NumberFormatException e) {
            throw new RcFileCorruptionException(e, "Invalid float value");
        }
    }
}
