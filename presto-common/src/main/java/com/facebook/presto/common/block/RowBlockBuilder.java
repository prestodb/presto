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

package com.facebook.presto.common.block;

import com.facebook.presto.common.type.Type;
import io.airlift.slice.SliceInput;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import static com.facebook.presto.common.block.BlockUtil.calculateBlockResetSize;
import static com.facebook.presto.common.block.RowBlock.createRowBlockInternal;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RowBlockBuilder
        extends AbstractRowBlock
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(RowBlockBuilder.class).instanceSize();

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;

    private int positionCount;
    private int[] fieldBlockOffsets;
    private boolean[] rowIsNull;
    private boolean hasNullRow;
    private final BlockBuilder[] fieldBlockBuilders;

    private boolean currentEntryOpened;

    public RowBlockBuilder(List<Type> fieldTypes, BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this(
                blockBuilderStatus,
                createFieldBlockBuilders(fieldTypes, blockBuilderStatus, expectedEntries),
                new int[expectedEntries + 1],
                new boolean[expectedEntries]);
    }

    private RowBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, BlockBuilder[] fieldBlockBuilders, int[] fieldBlockOffsets, boolean[] rowIsNull)
    {
        super(fieldBlockBuilders.length);

        this.blockBuilderStatus = blockBuilderStatus;
        this.positionCount = 0;
        this.fieldBlockOffsets = requireNonNull(fieldBlockOffsets, "fieldBlockOffsets is null");
        this.rowIsNull = requireNonNull(rowIsNull, "rowIsNull is null");
        this.fieldBlockBuilders = requireNonNull(fieldBlockBuilders, "fieldBlockBuilders is null");
    }

    private static BlockBuilder[] createFieldBlockBuilders(List<Type> fieldTypes, BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        // Stream API should not be used since constructor can be called in performance sensitive sections
        BlockBuilder[] fieldBlockBuilders = new BlockBuilder[fieldTypes.size()];
        for (int i = 0; i < fieldTypes.size(); i++) {
            fieldBlockBuilders[i] = fieldTypes.get(i).createBlockBuilder(blockBuilderStatus, expectedEntries);
        }
        return fieldBlockBuilders;
    }

    @Override
    protected Block[] getRawFieldBlocks()
    {
        return fieldBlockBuilders;
    }

    @Override
    protected int[] getFieldBlockOffsets()
    {
        return fieldBlockOffsets;
    }

    @Override
    public int getOffsetBase()
    {
        return 0;
    }

    @Nullable
    @Override
    protected boolean[] getRowIsNull()
    {
        return hasNullRow ? rowIsNull : null;
    }

    @Override
    public boolean mayHaveNull()
    {
        return hasNullRow;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return hasNullRow && rowIsNull[position];
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        long sizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) positionCount;
        for (int i = 0; i < numFields; i++) {
            sizeInBytes += fieldBlockBuilders[i].getSizeInBytes();
        }
        return sizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE + sizeOf(fieldBlockOffsets) + sizeOf(rowIsNull);
        for (int i = 0; i < numFields; i++) {
            size += fieldBlockBuilders[i].getRetainedSizeInBytes();
        }
        if (blockBuilderStatus != null) {
            size += BlockBuilderStatus.INSTANCE_SIZE;
        }
        return size;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        for (int i = 0; i < numFields; i++) {
            consumer.accept(fieldBlockBuilders[i], fieldBlockBuilders[i].getRetainedSizeInBytes());
        }
        consumer.accept(fieldBlockOffsets, sizeOf(fieldBlockOffsets));
        consumer.accept(rowIsNull, sizeOf(rowIsNull));
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public SingleRowBlockWriter beginBlockEntry()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Expected current entry to be closed but was opened");
        }
        currentEntryOpened = true;
        return new SingleRowBlockWriter(fieldBlockBuilders[0].getPositionCount(), fieldBlockBuilders);
    }

    @Override
    public BlockBuilder closeEntry()
    {
        if (!currentEntryOpened) {
            throw new IllegalStateException("Expected entry to be opened but was closed");
        }

        entryAdded(false);
        currentEntryOpened = false;
        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }

        entryAdded(true);
        return this;
    }

    @Override
    public BlockBuilder readPositionFrom(SliceInput input)
    {
        boolean isNull = input.readByte() == 0;
        if (isNull) {
            appendNull();
        }
        else {
            for (BlockBuilder blockBuilder : fieldBlockBuilders) {
                blockBuilder.readPositionFrom(input);
            }
            entryAdded(false);
        }
        return this;
    }

    private void entryAdded(boolean isNull)
    {
        if (rowIsNull.length <= positionCount) {
            int newSize = BlockUtil.calculateNewArraySize(rowIsNull.length);
            rowIsNull = Arrays.copyOf(rowIsNull, newSize);
            fieldBlockOffsets = Arrays.copyOf(fieldBlockOffsets, newSize + 1);
        }

        if (isNull) {
            fieldBlockOffsets[positionCount + 1] = fieldBlockOffsets[positionCount];
        }
        else {
            fieldBlockOffsets[positionCount + 1] = fieldBlockOffsets[positionCount] + 1;
        }
        rowIsNull[positionCount] = isNull;
        hasNullRow |= isNull;
        positionCount++;

        for (int i = 0; i < numFields; i++) {
            if (fieldBlockBuilders[i].getPositionCount() != fieldBlockOffsets[positionCount]) {
                throw new IllegalStateException(format("field %s has unexpected position count. Expected: %s, actual: %s", i, fieldBlockOffsets[positionCount], fieldBlockBuilders[i].getPositionCount()));
            }
        }

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Integer.BYTES + Byte.BYTES);
        }
    }

    @Override
    public Block build()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before the block can be built");
        }
        Block[] fieldBlocks = new Block[numFields];
        for (int i = 0; i < numFields; i++) {
            fieldBlocks[i] = fieldBlockBuilders[i].build();
        }
        return createRowBlockInternal(0, positionCount, hasNullRow ? rowIsNull : null, fieldBlockOffsets, fieldBlocks);
    }

    @Override
    public String toString()
    {
        return format("RowBlockBuilder(%d){numFields=%d, positionCount=%d", hashCode(), numFields, getPositionCount());
    }

    @Override
    public BlockBuilder appendStructure(Block block)
    {
        if (!(block instanceof AbstractSingleRowBlock)) {
            throw new IllegalStateException("Expected AbstractSingleRowBlock");
        }
        if (currentEntryOpened) {
            throw new IllegalStateException("Expected current entry to be closed but was opened");
        }
        currentEntryOpened = true;

        int blockPositionCount = block.getPositionCount();
        if (blockPositionCount != numFields) {
            throw new IllegalArgumentException(format("block position count (%s) is not equal to number of fields (%s)", blockPositionCount, numFields));
        }
        for (int i = 0; i < blockPositionCount; i++) {
            if (block.isNull(i)) {
                fieldBlockBuilders[i].appendNull();
            }
            else {
                block.writePositionTo(i, fieldBlockBuilders[i]);
            }
        }

        closeEntry();
        return this;
    }

    @Override
    public BlockBuilder appendStructureInternal(Block block, int position)
    {
        if (!(block instanceof AbstractRowBlock)) {
            throw new IllegalArgumentException();
        }

        AbstractRowBlock rowBlock = (AbstractRowBlock) block;
        BlockBuilder entryBuilder = this.beginBlockEntry();

        int fieldBlockOffset = rowBlock.getFieldBlockOffset(position);
        for (int i = 0; i < rowBlock.numFields; i++) {
            if (rowBlock.getRawFieldBlocks()[i].isNull(fieldBlockOffset)) {
                entryBuilder.appendNull();
            }
            else {
                rowBlock.getRawFieldBlocks()[i].writePositionTo(fieldBlockOffset, entryBuilder);
            }
        }

        closeEntry();
        return this;
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        int newSize = calculateBlockResetSize(getPositionCount());
        BlockBuilder[] newBlockBuilders = new BlockBuilder[numFields];
        for (int i = 0; i < numFields; i++) {
            newBlockBuilders[i] = fieldBlockBuilders[i].newBlockBuilderLike(blockBuilderStatus);
        }
        return new RowBlockBuilder(blockBuilderStatus, newBlockBuilders, new int[newSize + 1], new boolean[newSize]);
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        int newSize = max(calculateBlockResetSize(getPositionCount()), expectedEntries);
        BlockBuilder[] newBlockBuilders = new BlockBuilder[numFields];
        // We still calculate the new expected fieldBlockBuilders sizes because the positions could be null.
        int nestedExpectedEntries = BlockUtil.calculateNestedStructureResetSize(fieldBlockOffsets[positionCount], positionCount, expectedEntries);
        for (int i = 0; i < numFields; i++) {
            newBlockBuilders[i] = fieldBlockBuilders[i].newBlockBuilderLike(blockBuilderStatus, nestedExpectedEntries);
        }
        return new RowBlockBuilder(blockBuilderStatus, newBlockBuilders, new int[newSize + 1], new boolean[newSize]);
    }
}
