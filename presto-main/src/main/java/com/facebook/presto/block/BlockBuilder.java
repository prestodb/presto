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

import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.base.Charsets;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import static com.google.common.base.Preconditions.checkNotNull;

public class BlockBuilder
{
    public static final DataSize DEFAULT_MAX_BLOCK_SIZE = new DataSize(64, Unit.KILOBYTE);
    public static final double DEFAULT_STORAGE_MULTIPLIER = 1.2;

    private final TupleInfo tupleInfo;
    private final int maxBlockSize;
    private final SliceOutput sliceOutput;
    private int positionCount;

    private TupleInfo.Builder tupleBuilder;

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

        this.tupleInfo = tupleInfo;
        this.maxBlockSize = maxBlockSize;
        this.sliceOutput = sliceOutput;

        tupleBuilder = tupleInfo.builder(this.sliceOutput);
    }

    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
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
            tupleBuilder.appendNull();
        }
        else if (value instanceof Boolean) {
            tupleBuilder.append((Boolean) value);
        }
        else if (value instanceof Double || value instanceof Float) {
            tupleBuilder.append(((Number) value).doubleValue());
        }
        else if (value instanceof Number) {
            tupleBuilder.append(((Number) value).longValue());
        }
        else if (value instanceof byte[]) {
            tupleBuilder.append(Slices.wrappedBuffer((byte[]) value));
        }
        else if (value instanceof String) {
            tupleBuilder.append((String) value);
        }
        else if (value instanceof Slice) {
            tupleBuilder.append((Slice) value);
        }
        else {
            throw new IllegalArgumentException("Unsupported type: " + value.getClass());
        }
        positionCount++;
        return this;
    }

    public BlockBuilder append(boolean value)
    {
        tupleBuilder.append(value);
        positionCount++;
        return this;
    }

    public BlockBuilder append(long value)
    {
        tupleBuilder.append(value);
        positionCount++;
        return this;
    }

    public BlockBuilder append(double value)
    {
        tupleBuilder.append(value);
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
        tupleBuilder.append(value);
        positionCount++;
        return this;
    }

    public BlockBuilder appendNull()
    {
        tupleBuilder.appendNull();
        positionCount++;
        return this;
    }

    public BlockBuilder append(TupleReadable tuple)
    {
        tupleBuilder.append(tuple);
        positionCount++;
        return this;
    }

    public BlockBuilder appendTuple(Slice slice, int offset)
    {
        // read the tuple length
        int length = tupleInfo.size(slice, offset);
        return appendTuple(slice, offset, length);
    }

    public BlockBuilder appendTuple(Slice slice, int offset, int length)
    {
        // copy tuple to output
        sliceOutput.writeBytes(slice, offset, length);
        positionCount++;

        return this;
    }

    public UncompressedBlock build()
    {
        return new UncompressedBlock(positionCount, tupleInfo, sliceOutput.slice());
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("BlockBuilder");
        sb.append("{count=").append(positionCount);
        sb.append(", size=").append(sliceOutput.size());
        sb.append(", maxSize=").append(maxBlockSize);
        sb.append(", tupleInfo=").append(tupleInfo);
        sb.append('}');
        return sb.toString();
    }
}
