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
package com.facebook.presto.operator.unnest;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.ColumnarArray;
import com.facebook.presto.spi.type.Type;

import static com.facebook.presto.spi.block.ColumnarArray.toColumnarArray;
import static com.google.common.base.Preconditions.checkState;

/**
 * Unnester for a nested column with array type, only when array elements are NOT of type {@code RowType}.
 * Maintains a {@link ColumnarArray} object to get underlying elements block from the array block.
 *
 * All protected methods implemented here assume that they are being invoked when {@code columnarArray} is non-null.
 */
class ArrayUnnester
        extends Unnester
{
    private ColumnarArray columnarArray;

    public ArrayUnnester(Type elementType)
    {
        super(elementType);
    }

    @Override
    public int getChannelCount()
    {
        return 1;
    }

    @Override
    protected int getInputEntryCount()
    {
        if (columnarArray == null) {
            return 0;
        }
        return columnarArray.getPositionCount();
    }

    @Override
    protected void resetColumnarStructure(Block block)
    {
        this.columnarArray = toColumnarArray(block);
    }

    @Override
    protected Block getElementsBlock(int channel)
    {
        checkState(channel == 0, "index is not 0");
        return columnarArray.getElementsBlock();
    }

    @Override
    protected void processCurrentPosition(int requiredOutputCount)
    {
        // Translate indices
        int startElementIndex = columnarArray.getOffset(getCurrentPosition());
        int length = columnarArray.getLength(getCurrentPosition());

        // Append elements and nulls
        getBlockBuilder(0).appendRange(startElementIndex, length);
        for (int i = 0; i < requiredOutputCount - length; i++) {
            getBlockBuilder(0).appendNull();
        }
    }

    @Override
    protected int getElementsLength(int index)
    {
        return columnarArray.getLength(index);
    }
}
