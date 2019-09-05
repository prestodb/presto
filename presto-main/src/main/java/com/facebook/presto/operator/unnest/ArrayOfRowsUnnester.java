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
import com.facebook.presto.spi.block.ColumnarRow;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.Iterables;

import static com.facebook.presto.spi.block.ColumnarArray.toColumnarArray;
import static com.facebook.presto.spi.block.ColumnarRow.toColumnarRow;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Unnester for a nested column with array type, only when array elements are of {@code RowType} type.
 * It maintains {@link ColumnarArray} and {@link ColumnarRow} objects to get underlying elements. The two
 * different columnar structures are required because there are two layers of translation involved. One
 * from {@code ArrayBlock} to {@code RowBlock}, and then from {@code RowBlock} to individual element blocks.
 *
 * All protected methods implemented here assume that they are invoked when {@code columnarArray} and
 * {@code columnarRow} are non-null.
 */
class ArrayOfRowsUnnester
        extends Unnester
{
    private ColumnarArray columnarArray;
    private ColumnarRow columnarRow;
    private final int fieldCount;

    // Keeping track of null row element count is required. This count needs to be deducted
    // when translating row block indexes to element block indexes.
    private int nullRowsEncountered;

    public ArrayOfRowsUnnester(RowType elementType)
    {
        super(Iterables.toArray(requireNonNull(elementType, "elementType is null").getTypeParameters(), Type.class));
        this.fieldCount = elementType.getTypeParameters().size();
        this.nullRowsEncountered = 0;
    }

    @Override
    public int getChannelCount()
    {
        return fieldCount;
    }

    @Override
    int getInputEntryCount()
    {
        if (columnarArray == null) {
            return 0;
        }
        return columnarArray.getPositionCount();
    }

    @Override
    protected void resetColumnarStructure(Block block)
    {
        columnarArray = toColumnarArray(block);
        columnarRow = toColumnarRow(columnarArray.getElementsBlock());
        nullRowsEncountered = 0;
    }

    @Override
    public void processCurrentPosition(int requireCount)
    {
        // Translate to row block index
        int rowBlockIndex = columnarArray.getOffset(getCurrentPosition());

        // Unnest current entry
        for (int i = 0; i < getCurrentUnnestedLength(); i++) {
            if (columnarRow.isNull(rowBlockIndex + i)) {
                // Nulls have to be appended when Row element itself is null
                for (int field = 0; field < fieldCount; field++) {
                    getBlockBuilder(field).appendNull();
                }
                nullRowsEncountered++;
            }
            else {
                for (int field = 0; field < fieldCount; field++) {
                    getBlockBuilder(field).appendElement(rowBlockIndex + i - nullRowsEncountered);
                }
            }
        }

        // Append nulls if more output entries are needed
        for (int i = 0; i < requireCount - getCurrentUnnestedLength(); i++) {
            for (int field = 0; field < fieldCount; field++) {
                getBlockBuilder(field).appendNull();
            }
        }
    }

    @Override
    protected Block getElementsBlock(int channel)
    {
        checkState(channel >= 0 && channel < fieldCount, "Invalid channel number");
        return columnarRow.getField(channel);
    }

    @Override
    protected int getElementsLength(int index)
    {
        return columnarArray.getLength(index);
    }
}
