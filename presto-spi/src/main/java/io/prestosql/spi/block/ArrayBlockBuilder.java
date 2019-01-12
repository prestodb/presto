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
import java.util.function.BiConsumer;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.spi.block.ArrayBlock.createArrayBlockInternal;
import static io.prestosql.spi.block.BlockUtil.calculateBlockResetSize;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class ArrayBlockBuilder
        extends AbstractArrayBlock
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ArrayBlockBuilder.class).instanceSize();

    private int positionCount;

    @Nullable
    private BlockBuilderStatus blockBuilderStatus;
    private boolean initialized;
    private int initialEntryCount;

    private int[] offsets = new int[1];
    private boolean[] valueIsNull = new boolean[0];
    private boolean hasNullValue;

    private final BlockBuilder values;
    private boolean currentEntryOpened;

    private long retainedSizeInBytes;

    /**
     * Caller of this constructor is responsible for making sure `valuesBlock` is constructed with the same `blockBuilderStatus` as the one in the argument
     */
    public ArrayBlockBuilder(BlockBuilder valuesBlock, BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this(
                blockBuilderStatus,
                valuesBlock,
                expectedEntries);
    }

    public ArrayBlockBuilder(Type elementType, BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        this(
                blockBuilderStatus,
                elementType.createBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry),
                expectedEntries);
    }

    public ArrayBlockBuilder(Type elementType, BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this(
                blockBuilderStatus,
                elementType.createBlockBuilder(blockBuilderStatus, expectedEntries),
                expectedEntries);
    }

    /**
     * Caller of this private constructor is responsible for making sure `values` is constructed with the same `blockBuilderStatus` as the one in the argument
     */
    private ArrayBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, BlockBuilder values, int expectedEntries)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.values = requireNonNull(values, "values is null");
        this.initialEntryCount = max(expectedEntries, 1);

        updateDataSize();
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        return values.getSizeInBytes() + ((Integer.BYTES + Byte.BYTES) * (long) positionCount);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes + values.getRetainedSizeInBytes();
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(values, values.getRetainedSizeInBytes());
        consumer.accept(offsets, sizeOf(offsets));
        consumer.accept(valueIsNull, sizeOf(valueIsNull));
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    protected Block getRawElementBlock()
    {
        return values;
    }

    @Override
    protected int[] getOffsets()
    {
        return offsets;
    }

    @Override
    protected int getOffsetBase()
    {
        return 0;
    }

    @Override
    protected boolean[] getValueIsNull()
    {
        return valueIsNull;
    }

    @Override
    public BlockBuilder appendStructure(Block block)
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Expected current entry to be closed but was opened");
        }
        currentEntryOpened = true;

        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                values.appendNull();
            }
            else {
                block.writePositionTo(i, values);
            }
        }

        closeEntry();
        return this;
    }

    @Override
    public BlockBuilder appendStructureInternal(Block block, int position)
    {
        if (!(block instanceof AbstractArrayBlock)) {
            throw new IllegalArgumentException();
        }

        AbstractArrayBlock arrayBlock = (AbstractArrayBlock) block;
        BlockBuilder entryBuilder = beginBlockEntry();

        int startValueOffset = arrayBlock.getOffset(position);
        int endValueOffset = arrayBlock.getOffset(position + 1);
        for (int i = startValueOffset; i < endValueOffset; i++) {
            if (arrayBlock.getRawElementBlock().isNull(i)) {
                entryBuilder.appendNull();
            }
            else {
                arrayBlock.getRawElementBlock().writePositionTo(i, entryBuilder);
            }
        }

        closeEntry();
        return this;
    }

    @Override
    public SingleArrayBlockWriter beginBlockEntry()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Expected current entry to be closed but was closed");
        }
        currentEntryOpened = true;
        return new SingleArrayBlockWriter(values, values.getPositionCount());
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
        if (valueIsNull.length <= positionCount) {
            growCapacity();
        }
        offsets[positionCount + 1] = values.getPositionCount();
        valueIsNull[positionCount] = isNull;
        hasNullValue |= isNull;
        positionCount++;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Integer.BYTES + Byte.BYTES);
        }
    }

    private void growCapacity()
    {
        int newSize;
        if (initialized) {
            newSize = BlockUtil.calculateNewArraySize(valueIsNull.length);
        }
        else {
            newSize = initialEntryCount;
            initialized = true;
        }

        valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        offsets = Arrays.copyOf(offsets, newSize + 1);
        updateDataSize();
    }

    private void updateDataSize()
    {
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(offsets);
        if (blockBuilderStatus != null) {
            retainedSizeInBytes += BlockBuilderStatus.INSTANCE_SIZE;
        }
    }

    @Override
    public ArrayBlock build()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before the block can be built");
        }
        return createArrayBlockInternal(0, positionCount, hasNullValue ? valueIsNull : null, offsets, values.build());
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        int newSize = calculateBlockResetSize(getPositionCount());
        return new ArrayBlockBuilder(blockBuilderStatus, values.newBlockBuilderLike(blockBuilderStatus), newSize);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ArrayBlockBuilder{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }
}
