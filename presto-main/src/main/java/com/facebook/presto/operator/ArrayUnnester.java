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
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import static com.facebook.presto.spi.block.StructBuilder.readBlock;
import static com.google.common.base.Preconditions.checkNotNull;

public class ArrayUnnester
        extends Unnester
{
    private final Type elementType;
    private final Block arrayBlock;

    private int position;
    private final int positionCount;

    public ArrayUnnester(ArrayType arrayType, @Nullable Slice slice)
    {
        super(1);
        this.elementType = checkNotNull(arrayType, "arrayType is null").getElementType();

        if (slice == null) {
            arrayBlock = null;
            positionCount = 0;
        }
        else {
            arrayBlock = readBlock(slice.getInput());
            positionCount = arrayBlock.getPositionCount();
        }
    }

    @Override
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
}
