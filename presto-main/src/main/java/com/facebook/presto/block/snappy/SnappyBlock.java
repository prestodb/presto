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
package com.facebook.presto.block.snappy;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedBlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedBooleanBlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedDoubleBlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedLongBlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedSliceBlockCursor;
import com.facebook.presto.serde.SnappyBlockEncoding;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.iq80.snappy.Snappy;

import javax.annotation.concurrent.GuardedBy;

import static com.google.common.base.Preconditions.checkState;

public class SnappyBlock
        implements Block
{
    private final int positionCount;
    private final TupleInfo tupleInfo;
    private final Slice compressedSlice;

    @GuardedBy("this")
    private Slice uncompressedSlice = null;

    public SnappyBlock(int positionCount, TupleInfo tupleInfo, Slice compressedSlice)
    {
        Preconditions.checkArgument(positionCount >= 0, "positionCount is negative");
        Preconditions.checkNotNull(tupleInfo, "tupleInfo is null");
        Preconditions.checkNotNull(compressedSlice, "compressedSlice is null");

        this.tupleInfo = tupleInfo;
        this.compressedSlice = compressedSlice;
        this.positionCount = positionCount;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    public Slice getCompressedSlice()
    {
        return compressedSlice;
    }

    public synchronized Slice getUncompressedSlice()
    {
        if (uncompressedSlice == null) {
            int uncompressedLength = Snappy.getUncompressedLength(compressedSlice.getBytes(), 0);
            checkState(uncompressedLength > 0, "Empty block encountered!");
            byte[] output = new byte[uncompressedLength];
            Snappy.uncompress(compressedSlice.getBytes(), 0, compressedSlice.length(), output, 0);
            uncompressedSlice = Slices.wrappedBuffer(output);
        }
        return uncompressedSlice;
    }

    public int getSliceOffset()
    {
        return 0;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public DataSize getDataSize()
    {
        return new DataSize(getUncompressedSlice().length(), Unit.BYTE);
    }

    @Override
    public BlockCursor cursor()
    {
        if (tupleInfo.getFieldCount() == 1) {
            Type type = tupleInfo.getTypes().get(0);
            if (type == Type.BOOLEAN) {
                return new UncompressedBooleanBlockCursor(positionCount, getUncompressedSlice());
            }
            if (type == Type.FIXED_INT_64) {
                return new UncompressedLongBlockCursor(positionCount, getUncompressedSlice());
            }
            if (type == Type.DOUBLE) {
                return new UncompressedDoubleBlockCursor(positionCount, getUncompressedSlice());
            }
            if (type == Type.VARIABLE_BINARY) {
                return new UncompressedSliceBlockCursor(positionCount, getUncompressedSlice());
            }
        }
        return new UncompressedBlockCursor(tupleInfo, positionCount, getUncompressedSlice());
    }

    @Override
    public SnappyBlockEncoding getEncoding()
    {
        return new SnappyBlockEncoding(tupleInfo);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        Preconditions.checkPositionIndexes(positionOffset, positionOffset + length, positionCount);
        return cursor().getRegionAndAdvance(length);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("positionCount", positionCount)
                .add("tupleInfo", tupleInfo)
                .add("compressedSlice", compressedSlice)
                .toString();
    }
}
