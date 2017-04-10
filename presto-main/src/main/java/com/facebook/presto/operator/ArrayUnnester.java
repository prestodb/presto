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
package com.facebook.presto.operator;

import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

public class ArrayUnnester
        implements Unnester
{
    private final Type elementType;
    private Block arrayBlock;
    private final int channelCount;

    private int position;
    private int positionCount;

    public ArrayUnnester(ArrayType arrayType, @Nullable Block arrayBlock)
    {
        this.channelCount = 1;
        this.elementType = requireNonNull(arrayType, "arrayType is null").getElementType();

        this.arrayBlock = arrayBlock;
        this.positionCount = arrayBlock == null ? 0 : arrayBlock.getPositionCount();
    }

    protected void appendTo(PageBuilder pageBuilder, int outputChannelOffset)
    {
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
        elementType.appendTo(arrayBlock, position, blockBuilder);
        position++;
    }

    @Override
    public boolean hasNext()
    {
        return position < positionCount;
    }

    @Override
    public final int getChannelCount()
    {
        return channelCount;
    }

    @Override
    public final void appendNext(PageBuilder pageBuilder, int outputChannelOffset)
    {
        appendTo(pageBuilder, outputChannelOffset);
    }

    @Override
    public void setBlock(@Nullable Block arrayBlock)
    {
        this.arrayBlock = arrayBlock;
        this.position = 0;
        this.positionCount = arrayBlock == null ? 0 : arrayBlock.getPositionCount();
    }
}
