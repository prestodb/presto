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

package io.prestosql.spi.block;

import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.spi.block.BlockUtil.calculateBlockResetSize;
import static io.prestosql.spi.block.RowBlock.createRowBlockInternal;
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
    protected int getOffsetBase()
    {
        return 0;
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
        return createRowBlockInternal(0, positionCount, rowIsNull, fieldBlockOffsets, fieldBlocks);
    }

    @Override
    public String toString()
    {
        return format("RowBlockBuilder{numFields=%d, positionCount=%d", numFields, getPositionCount());
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
}
