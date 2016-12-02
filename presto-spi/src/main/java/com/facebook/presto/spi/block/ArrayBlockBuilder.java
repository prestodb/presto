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

import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static com.facebook.presto.spi.block.BlockUtil.calculateBlockResetSize;
import static com.facebook.presto.spi.block.BlockUtil.intSaturatedCast;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class ArrayBlockBuilder
        extends AbstractArrayBlock
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ArrayBlockBuilder.class).instanceSize() + BlockBuilderStatus.INSTANCE_SIZE;

    private int positionCount;

    private BlockBuilderStatus blockBuilderStatus;

    private int[] offsets;
    private boolean[] valueIsNull;

    private final BlockBuilder values;
    private int currentEntrySize;

    private int retainedSizeInBytes;

    /**
     * Caller of this constructor is responsible for making sure `valuesBlock` is constructed with the same `blockBuilderStatus` as the one in the argument
     */
    public ArrayBlockBuilder(BlockBuilder valuesBlock, BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this(
                blockBuilderStatus,
                valuesBlock,
                new int[expectedEntries + 1],
                new boolean[expectedEntries]);
    }

    public ArrayBlockBuilder(Type elementType, BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        this(
                blockBuilderStatus,
                elementType.createBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry),
                new int[expectedEntries + 1],
                new boolean[expectedEntries]);
    }

    public ArrayBlockBuilder(Type elementType, BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this(
                blockBuilderStatus,
                elementType.createBlockBuilder(blockBuilderStatus, expectedEntries),
                new int[expectedEntries + 1],
                new boolean[expectedEntries]);
    }

    /**
     * Caller of this private constructor is responsible for making sure `values` is constructed with the same `blockBuilderStatus` as the one in the argument
     */
    private ArrayBlockBuilder(BlockBuilderStatus blockBuilderStatus, BlockBuilder values, int[] offsets, boolean[] valueIsNull)
    {
        this.blockBuilderStatus = requireNonNull(blockBuilderStatus, "blockBuilderStatus is null");
        this.values = requireNonNull(values, "values is null");
        this.offsets = requireNonNull(offsets, "offset is null");
        this.valueIsNull = requireNonNull(valueIsNull, "valueIsNull is null");
        if (offsets.length != valueIsNull.length + 1) {
            throw new IllegalArgumentException("expected offsets and valueIsNull to have same length");
        }

        updateDataSize();
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getSizeInBytes()
    {
        return values.getSizeInBytes() + ((Integer.BYTES + Byte.BYTES) * positionCount);
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    protected Block getValues()
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
    public BlockBuilder writeObject(Object value)
    {
        if (currentEntrySize != 0) {
            throw new IllegalStateException("Expected entry size to be exactly " + 0 + " but was " + currentEntrySize);
        }

        Block block = (Block) value;
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                values.appendNull();
            }
            else {
                block.writePositionTo(i, values);
                values.closeEntry();
            }
        }

        currentEntrySize++;
        return this;
    }

    @Override
    public ArrayElementBlockWriter beginBlockEntry()
    {
        if (currentEntrySize != 0) {
            throw new IllegalStateException("Expected current entry size to be exactly 0 but was " + currentEntrySize);
        }
        currentEntrySize++;
        return new ArrayElementBlockWriter(values, values.getPositionCount());
    }

    @Override
    public BlockBuilder closeEntry()
    {
        if (currentEntrySize != 1) {
            throw new IllegalStateException("Expected entry size to be exactly 1 but was " + currentEntrySize);
        }

        entryAdded(false);
        currentEntrySize = 0;
        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        if (currentEntrySize > 0) {
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
        positionCount++;

        blockBuilderStatus.addBytes(Integer.BYTES + Byte.BYTES);
    }

    private void growCapacity()
    {
        int newSize = BlockUtil.calculateNewArraySize(valueIsNull.length);
        valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        offsets = Arrays.copyOf(offsets, newSize + 1);
        updateDataSize();
    }

    private void updateDataSize()
    {
        retainedSizeInBytes = intSaturatedCast(INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(offsets));
    }

    @Override
    public ArrayBlock build()
    {
        if (currentEntrySize > 0) {
            throw new IllegalStateException("Current entry must be closed before the block can be built");
        }
        return new ArrayBlock(positionCount, valueIsNull, offsets, values.build());
    }

    @Override
    public void reset(BlockBuilderStatus blockBuilderStatus)
    {
        this.blockBuilderStatus = requireNonNull(blockBuilderStatus, "blockBuilderStatus is null");

        int newSize = calculateBlockResetSize(getPositionCount());
        valueIsNull = new boolean[newSize];
        offsets = new int[newSize + 1];
        values.reset(blockBuilderStatus);

        currentEntrySize = 0;
        positionCount = 0;

        updateDataSize();
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
