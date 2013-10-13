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

import com.facebook.presto.block.uncompressed.AbstractFixedWidthBlock;
import com.facebook.presto.block.uncompressed.FixedWidthBlock;
import com.facebook.presto.tuple.FixedWidthTypeInfo;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;

import static com.google.common.base.Preconditions.checkNotNull;

public class FixedWidthBlockBuilder
        extends AbstractFixedWidthBlock
        implements BlockBuilder
{
    private final int maxBlockSize;
    private final SliceOutput sliceOutput;
    private int positionCount;

    public FixedWidthBlockBuilder(FixedWidthTypeInfo typeInfo, DataSize maxBlockSize, DataSize initialBufferSize)
    {
        super(typeInfo);

        checkNotNull(maxBlockSize, "maxBlockSize is null");
        this.maxBlockSize = (int) maxBlockSize.toBytes();

        checkNotNull(initialBufferSize, "initialBufferSize is null");
        this.sliceOutput = new DynamicSliceOutput((int) initialBufferSize.toBytes());
    }

    public FixedWidthBlockBuilder(FixedWidthTypeInfo typeInfo, Slice slice)
    {
        super(typeInfo);

        this.maxBlockSize = slice.length();
        this.sliceOutput = slice.getOutput();
    }

    protected Slice getRawSlice()
    {
        return sliceOutput.getUnderlyingSlice();
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public boolean isEmpty()
    {
        return positionCount == 0;
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
    public int writableBytes()
    {
        return maxBlockSize - sliceOutput.size();
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
        positionCount++;
        sliceOutput.writeByte(0);
        typeInfo.setBoolean(sliceOutput, value);
        return this;
    }

    @Override
    public BlockBuilder append(long value)
    {
        positionCount++;
        sliceOutput.writeByte(0);
        typeInfo.setLong(sliceOutput, value);
        return this;
    }

    @Override
    public BlockBuilder append(double value)
    {
        positionCount++;
        sliceOutput.writeByte(0);
        typeInfo.setDouble(sliceOutput, value);
        return this;
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
        positionCount++;

        sliceOutput.writeByte(0);

        typeInfo.setSlice(sliceOutput, value, offset, length);

        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        positionCount++;

        sliceOutput.writeByte(1);

        // fixed width is always written regardless of null flag
        sliceOutput.writeZero(typeInfo.getSize());

        return this;
    }

    @Override
    public BlockBuilder appendTuple(Slice slice, int offset, int length)
    {
        positionCount++;

        // copy tuple to output
        sliceOutput.writeBytes(slice, offset, length);

        return this;
    }

    @Override
    public RandomAccessBlock build()
    {
        return new FixedWidthBlock(typeInfo, positionCount, sliceOutput.getUnderlyingSlice());
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("positionCount", positionCount)
                .add("size", sliceOutput.size())
                .add("maxSize", maxBlockSize)
                .add("typeInfo", typeInfo)
                .toString();
    }
}
