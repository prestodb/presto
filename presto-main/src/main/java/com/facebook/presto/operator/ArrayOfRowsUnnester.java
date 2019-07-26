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
import com.facebook.presto.spi.block.ColumnarArray;
import com.facebook.presto.spi.block.ColumnarRow;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import static com.google.common.base.Preconditions.checkArgument;
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
public class ArrayOfRowsUnnester
        implements Unnester
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
    public void appendNext(PageBuilder pageBuilder, int outputChannelOffset)
    {
        checkState(columnarRow != null, "columnarRow is null");

        for (int i = 0; i < fieldTypes.size(); i++) {
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset + i);
            if (columnarRow.isNull(position)) {
                blockBuilder.appendNull();
            }
            else {
                fieldTypes.get(i).appendTo(columnarRow.getField(i), nonNullPosition, blockBuilder);
            }
        }
        if (!columnarRow.isNull(position)) {
            nonNullPosition++;
        }
        position++;
    }

    @Override
    public boolean hasNext()
    {
        return position < positionCount;
    }

    @Override
    public void setBlock(Block block)
    {
        this.position = 0;
        this.nonNullPosition = 0;
        if (block == null) {
            this.columnarRow = null;
            this.positionCount = 0;
        }
        else {
            this.columnarRow = ColumnarRow.toColumnarRow(block);
            this.positionCount = block.getPositionCount();
        }
    }
}
