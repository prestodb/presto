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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockEncoding;
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
        implements Block
{
    private static final DataSize ENCODING_BUFFER_OVERHEAD = new DataSize(1, Unit.KILOBYTE);
    private final int positionCount;
    private final Slice compressedSlice;
    private final BlockEncoding uncompressedBlockEncoding;

    @GuardedBy("this")
    private Block uncompressedBlock;

    public SnappyBlock(int positionCount, Slice compressedSlice, BlockEncoding uncompressedBlockEncoding)
    {
        checkArgument(positionCount >= 0, "positionCount is negative");
        this.positionCount = positionCount;
        this.compressedSlice = checkNotNull(compressedSlice, "compressedSlice is null");
        this.uncompressedBlockEncoding = checkNotNull(uncompressedBlockEncoding, "uncompressedBlockEncoding is null");
    }

    public SnappyBlock(Block block)
    {
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

    public Slice getCompressedSlice()
    {
        return compressedSlice;
    }

    public synchronized Block getUncompressedBlock()
    {
        if (uncompressedBlock == null) {
            // decompress the slice
            int uncompressedLength = Snappy.getUncompressedLength(compressedSlice.getBytes(), 0);
            checkState(uncompressedLength > 0, "Empty block encountered!");
            byte[] output = new byte[uncompressedLength];
            Snappy.uncompress(compressedSlice.getBytes(), 0, compressedSlice.length(), output, 0);

            // decode the block
            uncompressedBlock = uncompressedBlockEncoding.readBlock(Slices.wrappedBuffer(output).getInput());
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
    public SnappyBlockEncoding getEncoding()
    {
        return new SnappyBlockEncoding(uncompressedBlockEncoding);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        return getUncompressedBlock().getRegion(positionOffset, length);
    }

    @Override
    public int getLength(int position)
    {
        return getUncompressedBlock().getLength(position);
    }

    @Override
    public byte getByte(int position, int offset)
    {
        return getUncompressedBlock().getByte(position, offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        return getUncompressedBlock().getShort(position, offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        return getUncompressedBlock().getInt(position, offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        return getUncompressedBlock().getLong(position, offset);
    }

    @Override
    public float getFloat(int position, int offset)
    {
        return getUncompressedBlock().getFloat(position, offset);
    }

    @Override
    public double getDouble(int position, int offset)
    {
        return getUncompressedBlock().getDouble(position, offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        return getUncompressedBlock().getSlice(position, offset, length);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        return getUncompressedBlock().bytesEqual(position, offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        return getUncompressedBlock().bytesCompare(position, offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        getUncompressedBlock().writeBytesTo(position, offset, length, blockBuilder);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        return getUncompressedBlock().equals(position, offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public int hash(int position, int offset, int length)
    {
        return getUncompressedBlock().hash(position, offset, length);
    }

    @Override
    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        return getUncompressedBlock().compareTo(leftPosition, leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        return getUncompressedBlock().getSingleValueBlock(position);
    }

    @Override
    public boolean isNull(int position)
    {
        return getUncompressedBlock().isNull(position);
    }

    @Override
    public void assureLoaded()
    {
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("positionCount", positionCount)
                .add("compressedSlice", compressedSlice)
                .toString();
    }
}
