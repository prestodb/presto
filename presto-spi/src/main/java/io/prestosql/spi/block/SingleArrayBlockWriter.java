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

public class SingleArrayBlockWriter
        extends AbstractSingleArrayBlock
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleArrayBlockWriter.class).instanceSize();

    private final BlockBuilder blockBuilder;
    private final long initialBlockBuilderSize;
    private int positionsWritten;

    public SingleArrayBlockWriter(BlockBuilder blockBuilder, int start)
    {
        super(start);
        this.blockBuilder = blockBuilder;
        this.initialBlockBuilderSize = blockBuilder.getSizeInBytes();
    }

    @Override
    protected Block getBlock()
    {
        return blockBuilder;
    }

    @Override
    public long getSizeInBytes()
    {
        return blockBuilder.getSizeInBytes() - initialBlockBuilderSize;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + blockBuilder.getRetainedSizeInBytes();
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(blockBuilder, blockBuilder.getRetainedSizeInBytes());
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public BlockBuilder writeByte(int value)
    {
        blockBuilder.writeByte(value);
        return this;
    }

    @Override
    public BlockBuilder writeShort(int value)
    {
        blockBuilder.writeShort(value);
        return this;
    }

    @Override
    public BlockBuilder writeInt(int value)
    {
        blockBuilder.writeInt(value);
        return this;
    }

    @Override
    public BlockBuilder writeLong(long value)
    {
        blockBuilder.writeLong(value);
        return this;
    }

    @Override
    public BlockBuilder writeBytes(Slice source, int sourceIndex, int length)
    {
        blockBuilder.writeBytes(source, sourceIndex, length);
        return this;
    }

    @Override
    public BlockBuilder appendStructure(Block block)
    {
        blockBuilder.appendStructure(block);
        entryAdded();
        return this;
    }

    @Override
    public BlockBuilder appendStructureInternal(Block block, int position)
    {
        blockBuilder.appendStructureInternal(block, position);
        entryAdded();
        return this;
    }

    @Override
    public BlockBuilder beginBlockEntry()
    {
        return blockBuilder.beginBlockEntry();
    }

    @Override
    public BlockBuilder appendNull()
    {
        blockBuilder.appendNull();
        entryAdded();
        return this;
    }

    @Override
    public BlockBuilder closeEntry()
    {
        blockBuilder.closeEntry();
        entryAdded();
        return this;
    }

    private void entryAdded()
    {
        positionsWritten++;
    }

    @Override
    public int getPositionCount()
    {
        return positionsWritten;
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
        return format("SingleArrayBlockWriter{positionCount=%d}", getPositionCount());
    }
}
