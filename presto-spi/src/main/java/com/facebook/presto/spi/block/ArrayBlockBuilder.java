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
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import static java.util.Objects.requireNonNull;

public class ArrayBlockBuilder
        extends AbstractArrayBlock
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ArrayBlockBuilder.class).instanceSize() + BlockBuilderStatus.INSTANCE_SIZE;

    private final BlockBuilderStatus blockBuilderStatus;
    private final BlockBuilder values;
    private final SliceOutput offsets;
    private final SliceOutput valueIsNull;
    private static final int OFFSET_BASE = 0;
    private int currentEntrySize;

    /**
     * Caller of this constructor is responsible for making sure `valuesBlock` is constructed with the same `blockBuilderStatus` as the one in the argument
     */
    public ArrayBlockBuilder(BlockBuilder valuesBlock, BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this(
                blockBuilderStatus,
                valuesBlock,
                new DynamicSliceOutput(expectedEntries * 4),
                new DynamicSliceOutput(expectedEntries));
    }

    public ArrayBlockBuilder(Type elementType, BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        this(
                blockBuilderStatus,
                elementType.createBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry),
                new DynamicSliceOutput(expectedEntries * 4),
                new DynamicSliceOutput(expectedEntries));
    }

    public ArrayBlockBuilder(Type elementType, BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this(
                blockBuilderStatus,
                elementType.createBlockBuilder(blockBuilderStatus, expectedEntries),
                new DynamicSliceOutput(expectedEntries * 4),
                new DynamicSliceOutput(expectedEntries));
    }

    /**
     * Caller of this private constructor is responsible for making sure `values` is constructed with the same `blockBuilderStatus` as the one in the argument
     */
    private ArrayBlockBuilder(BlockBuilderStatus blockBuilderStatus, BlockBuilder values, SliceOutput offsets, SliceOutput valueIsNull)
    {
        this.blockBuilderStatus = requireNonNull(blockBuilderStatus, "blockBuilderStatus is null");
        this.values = requireNonNull(values, "values is null");
        this.offsets = requireNonNull(offsets, "offset is null");
        this.valueIsNull = requireNonNull(valueIsNull);
    }

    @Override
    public int getPositionCount()
    {
        return valueIsNull.size();
    }

    @Override
    public int getSizeInBytes()
    {
        return values.getSizeInBytes() + offsets.size() + valueIsNull.size();
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + values.getRetainedSizeInBytes() + offsets.getUnderlyingSlice().getRetainedSize() + valueIsNull.getUnderlyingSlice().getRetainedSize();
    }

    @Override
    protected Block getValues()
    {
        return values;
    }

    @Override
    protected Slice getOffsets()
    {
        return offsets.getUnderlyingSlice();
    }

    @Override
    protected int getOffsetBase()
    {
        return OFFSET_BASE;
    }

    @Override
    protected Slice getValueIsNull()
    {
        return valueIsNull.getUnderlyingSlice();
    }

    @Override
    public void assureLoaded()
    {
    }

    @Override
    public BlockBuilder writeByte(int value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder writeShort(int value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder writeInt(int value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder writeLong(long value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder writeFloat(float value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder writeDouble(double value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder writeBytes(Slice source, int sourceIndex, int length)
    {
        throw new UnsupportedOperationException();
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
        offsets.appendInt(values.getPositionCount());
        valueIsNull.appendByte(isNull ? 1 : 0);

        blockBuilderStatus.addBytes(Integer.BYTES + Byte.BYTES);
    }

    @Override
    public ArrayBlock build()
    {
        if (currentEntrySize > 0) {
            throw new IllegalStateException("Current entry must be closed before the block can be built");
        }
        return new ArrayBlock(values.build(), offsets.slice(), OFFSET_BASE, valueIsNull.slice());
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
