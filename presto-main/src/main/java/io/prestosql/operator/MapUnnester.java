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

public class MapUnnester
        implements Unnester
{
    private final Type keyType;
    private final Type valueType;
    private Block block;

    private int position;
    private int positionCount;

    public MapUnnester(Type keyType, Type valueType)
    {
        this.keyType = requireNonNull(keyType, "keyType is null");
        this.valueType = requireNonNull(valueType, "valueType is null");
    }

    @Override
    public boolean hasNext()
    {
        return position < positionCount;
    }

    @Override
    public final int getChannelCount()
    {
        return 2;
    }

    @Override
    public final void appendNext(PageBuilder pageBuilder, int outputChannelOffset)
    {
        checkState(block != null, "block is null");
        BlockBuilder keyBlockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
        BlockBuilder valueBlockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset + 1);
        keyType.appendTo(block, position++, keyBlockBuilder);
        valueType.appendTo(block, position++, valueBlockBuilder);
    }

    @Override
    public void setBlock(@Nullable Block mapBlock)
    {
        this.block = mapBlock;
        this.position = 0;
        this.positionCount = mapBlock == null ? 0 : mapBlock.getPositionCount();
    }
}
