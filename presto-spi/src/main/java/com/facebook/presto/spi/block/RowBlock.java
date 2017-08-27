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
package com.facebook.presto.spi.block;

import org.openjdk.jol.info.ClassLayout;

import java.util.function.BiConsumer;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RowBlock
        extends AbstractRowBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(RowBlock.class).instanceSize();

    private final int startOffset;
    private final int positionCount;

    private final boolean[] rowIsNull;
    private final int[] fieldBlockOffsets;
    private final Block[] fieldBlocks;

    private volatile long sizeInBytes;
    private final long retainedSizeInBytes;

    public RowBlock(int startOffset, int positionCount, boolean[] rowIsNull, int[] fieldBlockOffsets, Block[] fieldBlocks)
    {
        super(fieldBlocks.length);

        this.startOffset = startOffset;
        this.positionCount = positionCount;
        this.rowIsNull = requireNonNull(rowIsNull, "rowIsNull is null");
        this.fieldBlockOffsets = requireNonNull(fieldBlockOffsets, "fieldBlockOffsets is null");
        this.fieldBlocks = requireNonNull(fieldBlocks, "fieldBlocks is null");
        int firstFieldBlockPositionCount = fieldBlocks[0].getPositionCount();
        for (int i = 1; i < fieldBlocks.length; i++) {
            if (firstFieldBlockPositionCount != fieldBlocks[i].getPositionCount()) {
                throw new IllegalArgumentException(format("length of field blocks differ: field 0: %s, block %s: %s", firstFieldBlockPositionCount, i, fieldBlocks[i].getPositionCount()));
            }
        }

        this.sizeInBytes = -1;
        long retainedSizeInBytes = INSTANCE_SIZE + sizeOf(fieldBlockOffsets) + sizeOf(rowIsNull);
        for (Block fieldBlock : fieldBlocks) {
            retainedSizeInBytes += fieldBlock.getRetainedSizeInBytes();
        }
        this.retainedSizeInBytes = retainedSizeInBytes;
    }

    @Override
    protected Block[] getFieldBlocks()
    {
        return fieldBlocks;
    }

    @Override
    protected int[] getFieldBlockOffsets()
    {
        return fieldBlockOffsets;
    }

    @Override
    protected int getOffsetBase()
    {
        return startOffset;
    }

    @Override
    protected boolean[] getRowIsNull()
    {
        return rowIsNull;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        if (sizeInBytes < 0) {
            calculateSize();
        }
        return sizeInBytes;
    }

    private void calculateSize()
    {
        int startFieldBlockOffset = fieldBlockOffsets[startOffset];
        int endFieldBlockOffset = fieldBlockOffsets[startOffset + positionCount];
        int fieldBlockLength = endFieldBlockOffset - startFieldBlockOffset;

        long sizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) positionCount;
        for (int i = 0; i < numFields; i++) {
            sizeInBytes += fieldBlocks[i].getRegionSizeInBytes(startFieldBlockOffset, fieldBlockLength);
        }
        this.sizeInBytes = sizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        for (int i = 0; i < numFields; i++) {
            consumer.accept(fieldBlocks[i], fieldBlocks[i].getRetainedSizeInBytes());
        }
        consumer.accept(fieldBlockOffsets, sizeOf(fieldBlockOffsets));
        consumer.accept(rowIsNull, sizeOf(rowIsNull));
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public String toString()
    {
        return format("RowBlock{numFields=%d, positionCount=%d}", numFields, getPositionCount());
    }
}
