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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.RandomAccessBlock;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Objects;
import com.google.common.primitives.Ints;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.iq80.snappy.Snappy;

import javax.annotation.concurrent.GuardedBy;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class SnappyBlock
        implements RandomAccessBlock
{
    private static final DataSize ENCODING_BUFFER_OVERHEAD = new DataSize(1, Unit.KILOBYTE);
    private final int positionCount;
    private final Type type;
    private final Slice compressedSlice;
    private final BlockEncoding uncompressedBlockEncoding;

    @GuardedBy("this")
    private RandomAccessBlock uncompressedBlock;

    public SnappyBlock(int positionCount, Type type, Slice compressedSlice, BlockEncoding uncompressedBlockEncoding)
    {
        this.type = checkNotNull(type, "type is null");
        checkArgument(positionCount >= 0, "positionCount is negative");
        this.positionCount = positionCount;
        this.compressedSlice = checkNotNull(compressedSlice, "compressedSlice is null");
        this.uncompressedBlockEncoding = checkNotNull(uncompressedBlockEncoding, "uncompressedBlockEncoding is null");
    }

    public SnappyBlock(RandomAccessBlock block)
    {
        type = block.getType();
        positionCount = block.getPositionCount();

        uncompressedBlock = block;
        uncompressedBlockEncoding = block.getEncoding();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(Ints.checkedCast(uncompressedBlock.getSizeInBytes() + ENCODING_BUFFER_OVERHEAD.toBytes()));
        uncompressedBlockEncoding.writeBlock(sliceOutput, uncompressedBlock);
        Slice uncompressedSlice = sliceOutput.slice();

        byte[] compressedBytes = new byte[Snappy.maxCompressedLength(uncompressedSlice.length())];
        int actualLength = Snappy.compress(uncompressedSlice.getBytes(), 0, uncompressedSlice.length(), compressedBytes, 0);
        compressedSlice = Slices.wrappedBuffer(Arrays.copyOf(compressedBytes, actualLength));
    }

    @Override
    public Type getType()
    {
        return type;
    }

    public Slice getCompressedSlice()
    {
        return compressedSlice;
    }

    public synchronized RandomAccessBlock getUncompressedBlock()
    {
        if (uncompressedBlock == null) {
            // decompress the slice
            int uncompressedLength = Snappy.getUncompressedLength(compressedSlice.getBytes(), 0);
            checkState(uncompressedLength > 0, "Empty block encountered!");
            byte[] output = new byte[uncompressedLength];
            Snappy.uncompress(compressedSlice.getBytes(), 0, compressedSlice.length(), output, 0);

            // decode the block
            uncompressedBlock = uncompressedBlockEncoding.readBlock(Slices.wrappedBuffer(output).getInput()).toRandomAccessBlock();
        }
        return uncompressedBlock;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getSizeInBytes()
    {
        return getUncompressedBlock().getSizeInBytes();
    }

    @Override
    public BlockCursor cursor()
    {
        return getUncompressedBlock().cursor();
    }

    @Override
    public SnappyBlockEncoding getEncoding()
    {
        return new SnappyBlockEncoding(type, uncompressedBlockEncoding);
    }

    @Override
    public RandomAccessBlock getRegion(int positionOffset, int length)
    {
        return getUncompressedBlock().getRegion(positionOffset, length);
    }

    @Override
    public boolean getBoolean(int position)
    {
        return getUncompressedBlock().getBoolean(position);
    }

    @Override
    public long getLong(int position)
    {
        return getUncompressedBlock().getLong(position);
    }

    @Override
    public double getDouble(int position)
    {
        return getUncompressedBlock().getDouble(position);
    }

    @Override
    public Slice getSlice(int position)
    {
        return getUncompressedBlock().getSlice(position);
    }

    @Override
    public Object getObjectValue(ConnectorSession session, int position)
    {
        return getUncompressedBlock().getObjectValue(session, position);
    }

    @Override
    public RandomAccessBlock getSingleValueBlock(int position)
    {
        return getUncompressedBlock().getSingleValueBlock(position);
    }

    @Override
    public boolean isNull(int position)
    {
        return getUncompressedBlock().isNull(position);
    }

    @Override
    public boolean equalTo(int position, RandomAccessBlock otherBlock, int otherPosition)
    {
        return getUncompressedBlock().equalTo(position, otherBlock, otherPosition);
    }

    @Override
    public boolean equalTo(int position, BlockCursor cursor)
    {
        return getUncompressedBlock().equalTo(position, cursor);
    }

    @Override
    public boolean equalTo(int position, Slice otherSlice, int otherOffset)
    {
        return getUncompressedBlock().equalTo(position, otherSlice, otherOffset);
    }

    @Override
    public int hash(int position)
    {
        return getUncompressedBlock().hash(position);
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, RandomAccessBlock otherBlock, int otherPosition)
    {
        return getUncompressedBlock().compareTo(sortOrder, position, otherBlock, otherPosition);
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, BlockCursor cursor)
    {
        return getUncompressedBlock().compareTo(sortOrder, position, cursor);
    }

    @Override
    public int compareTo(int position, Slice otherSlice, int otherOffset)
    {
        return getUncompressedBlock().compareTo(position, otherSlice, otherOffset);
    }

    @Override
    public void appendTo(int position, BlockBuilder blockBuilder)
    {
        getUncompressedBlock().appendTo(position, blockBuilder);
    }

    @Override
    public RandomAccessBlock toRandomAccessBlock()
    {
        return getUncompressedBlock();
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("positionCount", positionCount)
                .add("type", type)
                .add("compressedSlice", compressedSlice)
                .toString();
    }
}
