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

    /**
     * Create a row block directly from columnar nulls and field blocks.
     */
    public static Block fromFieldBlocks(boolean[] rowIsNull, Block[] fieldBlocks)
    {
        requireNonNull(rowIsNull, "rowIsNull is null");
        int[] fieldBlockOffsets = new int[rowIsNull.length + 1];
        for (int position = 0; position < rowIsNull.length; position++) {
            fieldBlockOffsets[position + 1] = fieldBlockOffsets[position] + (rowIsNull[position] ? 0 : 1);
        }
        validateConstructorArguments(0, rowIsNull.length, rowIsNull, fieldBlockOffsets, fieldBlocks);
        return new RowBlock(0, rowIsNull.length, rowIsNull, fieldBlockOffsets, fieldBlocks);
    }

    /**
     * Create a row block directly without per element validations.
     */
    static RowBlock createRowBlockInternal(int startOffset, int positionCount, boolean[] rowIsNull, int[] fieldBlockOffsets, Block[] fieldBlocks)
    {
        validateConstructorArguments(startOffset, positionCount, rowIsNull, fieldBlockOffsets, fieldBlocks);
        return new RowBlock(startOffset, positionCount, rowIsNull, fieldBlockOffsets, fieldBlocks);
    }

    private static void validateConstructorArguments(int startOffset, int positionCount, boolean[] rowIsNull, int[] fieldBlockOffsets, Block[] fieldBlocks)
    {
        if (startOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }

        requireNonNull(rowIsNull, "rowIsNull is null");
        if (rowIsNull.length - startOffset < positionCount) {
            throw new IllegalArgumentException("rowIsNull length is less than positionCount");
        }

        requireNonNull(fieldBlockOffsets, "fieldBlockOffsets is null");
        if (fieldBlockOffsets.length - startOffset < positionCount + 1) {
            throw new IllegalArgumentException("fieldBlockOffsets length is less than positionCount");
        }

        requireNonNull(fieldBlocks, "fieldBlocks is null");

        if (fieldBlocks.length <= 0) {
            throw new IllegalArgumentException("Number of fields in RowBlock must be positive");
        }

        int firstFieldBlockPositionCount = fieldBlocks[0].getPositionCount();
        for (int i = 1; i < fieldBlocks.length; i++) {
            if (firstFieldBlockPositionCount != fieldBlocks[i].getPositionCount()) {
                throw new IllegalArgumentException(format("length of field blocks differ: field 0: %s, block %s: %s", firstFieldBlockPositionCount, i, fieldBlocks[i].getPositionCount()));
            }
        }
    }

    /**
     * Use createRowBlockInternal or fromFieldBlocks instead of this method.  The caller of this method is assumed to have
     * validated the arguments with validateConstructorArguments.
     */
    private RowBlock(int startOffset, int positionCount, boolean[] rowIsNull, int[] fieldBlockOffsets, Block[] fieldBlocks)
    {
        super(fieldBlocks.length);

        this.startOffset = startOffset;
        this.positionCount = positionCount;
        this.rowIsNull = rowIsNull;
        this.fieldBlockOffsets = fieldBlockOffsets;
        this.fieldBlocks = fieldBlocks;

        this.sizeInBytes = -1;
        long retainedSizeInBytes = INSTANCE_SIZE + sizeOf(fieldBlockOffsets) + sizeOf(rowIsNull);
        for (Block fieldBlock : fieldBlocks) {
            retainedSizeInBytes += fieldBlock.getRetainedSizeInBytes();
        }
        this.retainedSizeInBytes = retainedSizeInBytes;
    }

    @Override
    protected Block[] getRawFieldBlocks()
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

    @Override
    public Block getLoadedBlock()
    {
        boolean allLoaded = true;
        Block[] loadedFieldBlocks = new Block[fieldBlocks.length];

        for (int i = 0; i < fieldBlocks.length; i++) {
            loadedFieldBlocks[i] = fieldBlocks[i].getLoadedBlock();
            if (loadedFieldBlocks[i] != fieldBlocks[i]) {
                allLoaded = false;
            }
        }

        if (allLoaded) {
            return this;
        }
        return createRowBlockInternal(
                startOffset,
                positionCount,
                rowIsNull,
                fieldBlockOffsets,
                loadedFieldBlocks);
    }
}
