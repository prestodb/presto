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

import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.util.function.BiConsumer;

import static java.lang.String.format;

public class SingleRowBlockWriter
        extends AbstractSingleRowBlock
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleRowBlockWriter.class).instanceSize();

    private final BlockBuilder[] fieldBlockBuilders;
    private final long initialBlockBuilderSize;
    private int positionsWritten;

    private int currentFieldIndexToWrite;
    private boolean fieldBlockBuilderReturned;

    SingleRowBlockWriter(int rowIndex, BlockBuilder[] fieldBlockBuilders)
    {
        super(rowIndex);
        this.fieldBlockBuilders = fieldBlockBuilders;
        long initialBlockBuilderSize = 0;
        for (BlockBuilder fieldBlockBuilder : fieldBlockBuilders) {
            initialBlockBuilderSize += fieldBlockBuilder.getSizeInBytes();
        }
        this.initialBlockBuilderSize = initialBlockBuilderSize;
    }

    /**
     * Obtains the field {@code BlockBuilder}.
     * <p>
     * This method is used to perform random write to {@code SingleRowBlockWriter}.
     * Each field {@code BlockBuilder} must be written EXACTLY once.
     * <p>
     * Field {@code BlockBuilder} can only be obtained before any sequential write has done.
     * Once obtained, sequential write is no longer allowed.
     */
    public BlockBuilder getFieldBlockBuilder(int fieldIndex)
    {
        if (currentFieldIndexToWrite != 0) {
            throw new IllegalStateException("field block builder can only be obtained before any sequential write has done");
        }
        fieldBlockBuilderReturned = true;
        return fieldBlockBuilders[fieldIndex];
    }

    @Override
    protected Block getRawFieldBlock(int fieldIndex)
    {
        return fieldBlockBuilders[fieldIndex];
    }

    @Override
    public long getSizeInBytes()
    {
        long currentBlockBuilderSize = 0;
        for (BlockBuilder fieldBlockBuilder : fieldBlockBuilders) {
            currentBlockBuilderSize += fieldBlockBuilder.getSizeInBytes();
        }
        return currentBlockBuilderSize - initialBlockBuilderSize;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE;
        for (BlockBuilder fieldBlockBuilder : fieldBlockBuilders) {
            size += fieldBlockBuilder.getRetainedSizeInBytes();
        }
        return size;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        for (BlockBuilder fieldBlockBuilder : fieldBlockBuilders) {
            consumer.accept(fieldBlockBuilder, fieldBlockBuilder.getRetainedSizeInBytes());
        }
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public BlockBuilder writeByte(int value)
    {
        checkFieldIndexToWrite();
        fieldBlockBuilders[currentFieldIndexToWrite].writeByte(value);
        return this;
    }

    @Override
    public BlockBuilder writeShort(int value)
    {
        checkFieldIndexToWrite();
        fieldBlockBuilders[currentFieldIndexToWrite].writeShort(value);
        return this;
    }

    @Override
    public BlockBuilder writeInt(int value)
    {
        checkFieldIndexToWrite();
        fieldBlockBuilders[currentFieldIndexToWrite].writeInt(value);
        return this;
    }

    @Override
    public BlockBuilder writeLong(long value)
    {
        checkFieldIndexToWrite();
        fieldBlockBuilders[currentFieldIndexToWrite].writeLong(value);
        return this;
    }

    @Override
    public BlockBuilder writeBytes(Slice source, int sourceIndex, int length)
    {
        checkFieldIndexToWrite();
        fieldBlockBuilders[currentFieldIndexToWrite].writeBytes(source, sourceIndex, length);
        return this;
    }

    @Override
    public BlockBuilder appendStructure(Block block)
    {
        checkFieldIndexToWrite();
        fieldBlockBuilders[currentFieldIndexToWrite].appendStructure(block);
        entryAdded();
        return this;
    }

    @Override
    public BlockBuilder appendStructureInternal(Block block, int position)
    {
        checkFieldIndexToWrite();
        fieldBlockBuilders[currentFieldIndexToWrite].appendStructureInternal(block, position);
        entryAdded();
        return this;
    }

    @Override
    public BlockBuilder beginBlockEntry()
    {
        checkFieldIndexToWrite();
        return fieldBlockBuilders[currentFieldIndexToWrite].beginBlockEntry();
    }

    @Override
    public BlockBuilder appendNull()
    {
        checkFieldIndexToWrite();
        fieldBlockBuilders[currentFieldIndexToWrite].appendNull();
        entryAdded();
        return this;
    }

    @Override
    public BlockBuilder closeEntry()
    {
        checkFieldIndexToWrite();
        fieldBlockBuilders[currentFieldIndexToWrite].closeEntry();
        entryAdded();
        return this;
    }

    private void entryAdded()
    {
        currentFieldIndexToWrite++;
        positionsWritten++;
    }

    @Override
    public int getPositionCount()
    {
        if (fieldBlockBuilderReturned) {
            throw new IllegalStateException("field block builder has been returned");
        }
        return positionsWritten;
    }

    @Override
    public String getEncodingName()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block build()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        if (!fieldBlockBuilderReturned) {
            return format("SingleRowBlockWriter{numFields=%d, fieldBlockBuilderReturned=false, positionCount=%d}", fieldBlockBuilders.length, getPositionCount());
        }
        else {
            return format("SingleRowBlockWriter{numFields=%d, fieldBlockBuilderReturned=true}", fieldBlockBuilders.length);
        }
    }

    private void checkFieldIndexToWrite()
    {
        if (fieldBlockBuilderReturned) {
            throw new IllegalStateException("cannot do sequential write after getFieldBlockBuilder is called");
        }
        if (currentFieldIndexToWrite >= fieldBlockBuilders.length) {
            throw new IllegalStateException("currentFieldIndexToWrite is not valid");
        }
    }
}
