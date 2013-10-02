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
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.RandomAccessBlock;
import com.facebook.presto.serde.UncompressedBlockEncoding;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class UncompressedBlock
        implements Block
{
    private final int positionCount;
    private final TupleInfo tupleInfo;
    private final Slice slice;

    public UncompressedBlock(int positionCount, TupleInfo tupleInfo, Slice slice)
    {
        checkArgument(positionCount >= 0, "positionCount is negative");
        checkNotNull(tupleInfo, "tupleInfo is null");
        checkNotNull(slice, "data is null");

        this.tupleInfo = tupleInfo;
        this.slice = slice;
        this.positionCount = positionCount;
    }

    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    public Slice getSlice()
    {
        return slice;
    }

    public int getSliceOffset()
    {
        return 0;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public DataSize getDataSize()
    {
        return new DataSize(slice.length(), Unit.BYTE);
    }

    @Override
    public BlockCursor cursor()
    {
        Type type = tupleInfo.getType();
        if (type == Type.BOOLEAN) {
            return new UncompressedBooleanBlockCursor(positionCount, slice);
        }
        else if (type == Type.FIXED_INT_64) {
            return new UncompressedLongBlockCursor(positionCount, slice);
        }
        else if (type == Type.DOUBLE) {
            return new UncompressedDoubleBlockCursor(positionCount, slice);
        }
        else if (type == Type.VARIABLE_BINARY) {
            return new UncompressedSliceBlockCursor(positionCount, slice);
        }
        throw new IllegalStateException("Unsupported type " + type);
    }

    @Override
    public UncompressedBlockEncoding getEncoding()
    {
        return new UncompressedBlockEncoding(tupleInfo);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        Preconditions.checkPositionIndexes(positionOffset, positionOffset + length, positionCount);
        return cursor().getRegionAndAdvance(length);
    }

    @Override
    public RandomAccessBlock toRandomAccessBlock()
    {
        Type type = tupleInfo.getType();
        if (type == Type.BOOLEAN) {
            return new UncompressedBooleanBlock(positionCount, slice);
        }
        if (type == Type.FIXED_INT_64) {
            return new UncompressedLongBlock(positionCount, slice);
        }
        if (type == Type.DOUBLE) {
            return new UncompressedDoubleBlock(positionCount, slice);
        }
        if (type == Type.VARIABLE_BINARY) {
            return new UncompressedSliceBlock(this);
        }
        throw new IllegalStateException("Unsupported type " + tupleInfo.getType());
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("positionCount", positionCount)
                .add("tupleInfo", tupleInfo)
                .add("slice", slice)
                .toString();
    }
}
