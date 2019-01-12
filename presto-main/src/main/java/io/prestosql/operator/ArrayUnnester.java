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

import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ArrayUnnester
        implements Unnester
{
    private final Type elementType;
    private Block arrayBlock;

    private int position;
    private int positionCount;

    public ArrayUnnester(Type elementType)
    {
        this.elementType = requireNonNull(elementType, "elementType is null");
    }

    @Override
    public boolean hasNext()
    {
        return position < positionCount;
    }

    @Override
    public final int getChannelCount()
    {
        return 1;
    }

    @Override
    public final void appendNext(PageBuilder pageBuilder, int outputChannelOffset)
    {
        checkState(arrayBlock != null, "arrayBlock is null");
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
        elementType.appendTo(arrayBlock, position, blockBuilder);
        position++;
    }

    @Override
    public void setBlock(@Nullable Block arrayBlock)
    {
        this.arrayBlock = arrayBlock;
        this.position = 0;
        this.positionCount = arrayBlock == null ? 0 : arrayBlock.getPositionCount();
    }
}
