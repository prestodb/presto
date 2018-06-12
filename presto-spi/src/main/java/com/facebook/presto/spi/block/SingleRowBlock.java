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

import static java.lang.String.format;

public class SingleRowBlock
        extends AbstractSingleRowBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleRowBlock.class).instanceSize();

    private final Block[] fieldBlocks;

    SingleRowBlock(int rowIndex, Block[] fieldBlocks)
    {
        super(rowIndex);
        this.fieldBlocks = fieldBlocks;
    }

    int getNumFields()
    {
        return fieldBlocks.length;
    }

    @Override
    protected Block getRawFieldBlock(int fieldIndex)
    {
        return fieldBlocks[fieldIndex];
    }

    @Override
    public int getPositionCount()
    {
        return fieldBlocks.length;
    }

    @Override
    public long getSizeInBytes()
    {
        long sizeInBytes = 0;
        for (int i = 0; i < fieldBlocks.length; i++) {
            sizeInBytes += getRawFieldBlock(i).getSizeInBytes();
        }
        return sizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long retainedSizeInBytes = INSTANCE_SIZE;
        for (int i = 0; i < fieldBlocks.length; i++) {
            retainedSizeInBytes += getRawFieldBlock(i).getRetainedSizeInBytes();
        }
        return retainedSizeInBytes;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        for (int i = 0; i < fieldBlocks.length; i++) {
            consumer.accept(fieldBlocks[i], fieldBlocks[i].getRetainedSizeInBytes());
        }
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        return SingleRowBlockEncoding.NAME;
    }

    public int getRowIndex()
    {
        return rowIndex;
    }

    @Override
    public String toString()
    {
        return format("SingleRowBlock{numFields=%d}", fieldBlocks.length);
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
        return new SingleRowBlock(rowIndex, loadedFieldBlocks);
    }
}
