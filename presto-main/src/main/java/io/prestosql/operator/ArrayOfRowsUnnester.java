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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.ColumnarRow;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ArrayOfRowsUnnester
        implements Unnester
{
    private final List<Type> fieldTypes;
    private ColumnarRow columnarRow;
    private int position;
    private int nonNullPosition;
    private int positionCount;

    public ArrayOfRowsUnnester(Type elementType)
    {
        requireNonNull(elementType, "elementType is null");
        checkArgument(elementType instanceof RowType, "elementType is not of RowType");
        this.fieldTypes = ImmutableList.copyOf(elementType.getTypeParameters());
    }

    @Override
    public int getChannelCount()
    {
        return fieldTypes.size();
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
