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
package com.facebook.presto.block;

import com.facebook.presto.block.uncompressed.AbstractVariableWidthRandomAccessBlock;
import com.facebook.presto.block.uncompressed.VariableWidthRandomAccessBlock;
import com.facebook.presto.type.Type;
import com.facebook.presto.type.VariableWidthType;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import static com.google.common.base.Preconditions.checkNotNull;

public class VariableWidthBlockBuilder
        extends AbstractVariableWidthRandomAccessBlock
        implements BlockBuilder
{
    private final int maxBlockSize;
    private final SliceOutput sliceOutput;
    private final IntArrayList offsets = new IntArrayList(1024);

    public VariableWidthBlockBuilder(VariableWidthType type, DataSize maxBlockSize, DataSize initialBufferSize)
    {
        super(type);

        checkNotNull(maxBlockSize, "maxBlockSize is null");
        this.maxBlockSize = (int) maxBlockSize.toBytes();

        checkNotNull(initialBufferSize, "initialBufferSize is null");
        this.sliceOutput = new DynamicSliceOutput((int) initialBufferSize.toBytes());
    }

    public VariableWidthBlockBuilder(VariableWidthType type, Slice slice)
    {
        super(type);
        this.maxBlockSize = slice.length();
        this.sliceOutput = slice.getOutput();
    }

    @Override
    protected int getPositionOffset(int position)
    {
        return offsets.getInt(position);
    }

    protected Slice getRawSlice()
    {
        return sliceOutput.getUnderlyingSlice();
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public int getPositionCount()
    {
        return offsets.size();
    }

    @Override
    public boolean isEmpty()
    {
        return offsets.isEmpty();
    }

    @Override
    public boolean isFull()
    {
        return sliceOutput.size() > maxBlockSize;
    }

    @Override
    public int size()
    {
        return sliceOutput.size();
    }

    @Override
    public BlockBuilder appendObject(Object value)
    {
        if (value == null) {
            appendNull();
        }
        else if (value instanceof Boolean) {
            append((Boolean) value);
        }
        else if (value instanceof Double || value instanceof Float) {
            append(((Number) value).doubleValue());
        }
        else if (value instanceof Number) {
            append(((Number) value).longValue());
        }
        else if (value instanceof byte[]) {
            append(Slices.wrappedBuffer((byte[]) value));
        }
        else if (value instanceof String) {
            append((String) value);
        }
        else if (value instanceof Slice) {
            append((Slice) value);
        }
        else {
            throw new IllegalArgumentException("Unsupported type: " + value.getClass());
        }
        return this;
    }

    @Override
    public BlockBuilder append(boolean value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder append(long value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder append(double value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder append(byte[] value)
    {
        return append(Slices.wrappedBuffer(value));
    }

    @Override
    public BlockBuilder append(String value)
    {
        return append(Slices.copiedBuffer(value, Charsets.UTF_8));
    }

    @Override
    public BlockBuilder append(Slice value)
    {
        return append(value, 0, value.length());
    }

    @Override
    public BlockBuilder append(Slice value, int offset, int length)
    {
        offsets.add(sliceOutput.size());

        sliceOutput.writeByte(0);

        type.setSlice(sliceOutput, value, offset, length);

        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        offsets.add(sliceOutput.size());
        sliceOutput.writeByte(1);
        return this;
    }

    @Override
    public RandomAccessBlock build()
    {
        return new VariableWidthRandomAccessBlock(type, sliceOutput.getUnderlyingSlice(), offsets.toIntArray());
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("positionCount", offsets.size())
                .add("size", sliceOutput.size())
                .add("maxSize", maxBlockSize)
                .add("type", type)
                .toString();
    }
}
