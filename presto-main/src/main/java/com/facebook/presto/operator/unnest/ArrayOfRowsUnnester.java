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
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;

import java.util.List;

import static com.facebook.presto.spi.block.ColumnarArray.toColumnarArray;
import static com.facebook.presto.spi.block.ColumnarRow.toColumnarRow;
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
        implements Unnester
{
    private final int fieldCount;
    private final UnnestBlockBuilder[] unnestBlockBuilders;

    private ColumnarArray columnarArray;
    private ColumnarRow columnarRow;
    private int currentPosition;

    // Keeping track of null row element count is required. This count needs to be deducted
    // when translating row block indexes to element block indexes.
    private int nullRowsEncountered;

    public ArrayOfRowsUnnester(RowType elementType)
    {
        fieldCount = elementType.getTypeParameters().size();
        unnestBlockBuilders = createUnnestBlockBuilders(elementType.getTypeParameters());
        currentPosition = 0;
        nullRowsEncountered = 0;
    }

    @Override
    public int getChannelCount()
    {
        return fieldCount;
    }

    @Override
    public void resetInput(Block block)
    {
        requireNonNull(block, "block is null");

        columnarArray = toColumnarArray(block);
        columnarRow = toColumnarRow(columnarArray.getElementsBlock());
        nullRowsEncountered = 0;
        currentPosition = 0;

        for (int i = 0; i < fieldCount; i++) {
            unnestBlockBuilders[i].resetInputBlock(columnarRow.getField(i));
        }
    }

    @Override
    public void startNewOutput(PageBuilderStatus status, int expectedEntries)
    {
        for (int i = 0; i < fieldCount; i++) {
            unnestBlockBuilders[i].startNewOutput(status, expectedEntries);
        }
    }

    @Override
    public int getCurrentUnnestedLength()
    {
        return columnarArray.getLength(currentPosition);
    }

    @Override
    public void processCurrentAndAdvance(int requiredOutputCount)
    {
        // Translate to row block index
        int rowBlockIndex = columnarArray.getOffset(currentPosition);

        // Unnest current entry
        for (int i = 0; i < getCurrentUnnestedLength(); i++) {
            if (columnarRow.isNull(rowBlockIndex + i)) {
                // Nulls have to be appended when Row element itself is null
                for (int field = 0; field < fieldCount; field++) {
                    unnestBlockBuilders[field].appendNull();
                }
                nullRowsEncountered++;
            }
            else {
                for (int field = 0; field < fieldCount; field++) {
                    unnestBlockBuilders[field].appendElement(rowBlockIndex + i - nullRowsEncountered);
                }
            }
        }

        // Append nulls if more output entries are needed
        for (int i = 0; i < requiredOutputCount - getCurrentUnnestedLength(); i++) {
            for (int field = 0; field < fieldCount; field++) {
                unnestBlockBuilders[field].appendNull();
            }
        }

        currentPosition++;
    }

    @Override
    public Block[] buildOutputBlocksAndFlush()
    {
        Block[] outputBlocks = new Block[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            outputBlocks[i] = unnestBlockBuilders[i].buildOutputAndFlush();
        }
        return outputBlocks;
    }

    private static UnnestBlockBuilder[] createUnnestBlockBuilders(List<Type> elementTypes)
    {
        int fieldCount = elementTypes.size();
        UnnestBlockBuilder[] builders = new UnnestBlockBuilder[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            builders[i] = new UnnestBlockBuilder(elementTypes.get(i));
        }

        return builders;
    }
}
