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

import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

public class ArrayElementBlockWriter
        extends AbstractArrayElementBlock
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ArrayElementBlockWriter.class).instanceSize();

    private final BlockBuilder blockBuilder;
    private final int initialBlockBuilderSize;
    private int positionsWritten;

    public ArrayElementBlockWriter(BlockBuilder blockBuilder, int start)
    {
        super(start);
        this.blockBuilder = blockBuilder;
        this.initialBlockBuilderSize = blockBuilder.getSizeInBytes();
    }

    @Override
    protected BlockBuilder getBlock()
    {
        return blockBuilder;
    }

    @Override
    public int getSizeInBytes()
    {
        return blockBuilder.getSizeInBytes() - initialBlockBuilderSize;
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + blockBuilder.getRetainedSizeInBytes();
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
    public BlockBuilder writeObject(Object value)
    {
        blockBuilder.writeObject(value);
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
    public void reset(BlockBuilderStatus blockBuilderStatus)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ArrayElementBlockWriter{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }
}
