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

import com.facebook.presto.block.uncompressed.FixedWidthBlock;
import com.facebook.presto.block.uncompressed.VariableWidthBlock;
import com.facebook.presto.tuple.FixedWidthTypeInfo;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TypeInfo;
import com.facebook.presto.tuple.VariableWidthTypeInfo;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class BlockBuilder
{
    public static final DataSize DEFAULT_MAX_BLOCK_SIZE = new DataSize(64, Unit.KILOBYTE);
    public static final double DEFAULT_STORAGE_MULTIPLIER = 1.2;

    private final TypeInfo typeInfo;
    private final int maxBlockSize;
    private final SliceOutput sliceOutput;
    private int positionCount;

    public BlockBuilder(TupleInfo tupleInfo)
    {
        this(tupleInfo, DEFAULT_MAX_BLOCK_SIZE, DEFAULT_STORAGE_MULTIPLIER);
    }

    public BlockBuilder(TupleInfo tupleInfo, DataSize blockSize, double storageMultiplier)
    {
        // Use slightly larger storage size to minimize resizing when we just exceed full capacity
        this(tupleInfo, (int) checkNotNull(blockSize, "blockSize is null").toBytes(), new DynamicSliceOutput((int) ((int) blockSize.toBytes() * storageMultiplier)));
    }

    public BlockBuilder(TupleInfo tupleInfo,
            int maxBlockSize,
            SliceOutput sliceOutput)
    {
        checkNotNull(maxBlockSize, "maxBlockSize is null");
        checkNotNull(tupleInfo, "tupleInfo is null");

        this.typeInfo = tupleInfo.getTypeInfo();
        this.maxBlockSize = maxBlockSize;
        this.sliceOutput = sliceOutput;
    }

    public Slice getSlice()
    {
        return sliceOutput.getUnderlyingSlice();
    }

    public TupleInfo getTupleInfo()
    {
        return new TupleInfo(typeInfo.getType());
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public boolean isEmpty()
    {
        return positionCount == 0;
    }

    public boolean isFull()
    {
        return sliceOutput.size() > maxBlockSize;
    }

    public int size()
    {
        return sliceOutput.size();
    }

    public int writableBytes()
    {
        return maxBlockSize - sliceOutput.size();
    }

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

    public BlockBuilder append(boolean value)
    {
        checkState(typeInfo instanceof FixedWidthTypeInfo, "Expected fixed width type");

        sliceOutput.writeByte(0);
        ((FixedWidthTypeInfo) typeInfo).setBoolean(sliceOutput, value);
        positionCount++;
        return this;
    }

    public BlockBuilder append(long value)
    {
        checkState(typeInfo instanceof FixedWidthTypeInfo, "Expected fixed width type");

        sliceOutput.writeByte(0);
        ((FixedWidthTypeInfo) typeInfo).setLong(sliceOutput, value);
        positionCount++;
        return this;
    }

    public BlockBuilder append(double value)
    {
        checkState(typeInfo instanceof FixedWidthTypeInfo, "Expected fixed width type");

        sliceOutput.writeByte(0);
        ((FixedWidthTypeInfo) typeInfo).setDouble(sliceOutput, value);
        positionCount++;
        return this;
    }

    public BlockBuilder append(byte[] value)
    {
        return append(Slices.wrappedBuffer(value));
    }

    public BlockBuilder append(String value)
    {
        return append(Slices.copiedBuffer(value, Charsets.UTF_8));
    }

    public BlockBuilder append(Slice value)
    {
        return append(value, 0, value.length());
    }

    public BlockBuilder append(Slice value, int offset, int length)
    {
        sliceOutput.writeByte(0);

        if (typeInfo instanceof FixedWidthTypeInfo) {
            ((FixedWidthTypeInfo) typeInfo).setSlice(sliceOutput, value, offset, length);
        }
        else {
            ((VariableWidthTypeInfo) typeInfo).setSlice(sliceOutput, value, offset, length);
        }

        positionCount++;
        return this;
    }

    public BlockBuilder appendNull()
    {
        sliceOutput.writeByte(1);

        // fixed width is always written regardless of null flag
        if (typeInfo instanceof FixedWidthTypeInfo) {
            FixedWidthTypeInfo info = (FixedWidthTypeInfo) typeInfo;
            sliceOutput.writeZero(info.getSize());
        }

        positionCount++;
        return this;
    }

    public BlockBuilder appendTuple(Slice slice, int offset, int length)
    {
        // copy tuple to output
        sliceOutput.writeBytes(slice, offset, length);
        positionCount++;

        return this;
    }

    public Block build()
    {
        if (typeInfo instanceof FixedWidthTypeInfo) {
            return new FixedWidthBlock((FixedWidthTypeInfo) typeInfo, positionCount, sliceOutput.getUnderlyingSlice());
        }
        else {
            return new VariableWidthBlock((VariableWidthTypeInfo) typeInfo, positionCount, sliceOutput.getUnderlyingSlice());
        }
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
