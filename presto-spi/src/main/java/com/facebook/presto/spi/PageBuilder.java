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
package com.facebook.presto.spi;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.type.Type;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.block.BlockBuilderStatus.DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;
import static com.facebook.presto.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class PageBuilder
{
    private final BlockBuilder[] blockBuilders;
    private final List<Type> types;
    private PageBuilderStatus pageBuilderStatus;
    private int declaredPositions;

    public PageBuilder(List<? extends Type> types)
    {
        this.types = unmodifiableList(new ArrayList<>(requireNonNull(types, "types is null")));
        int maxBlockSizeInBytes;
        if (!types.isEmpty()) {
            maxBlockSizeInBytes = (int) (1.0 * DEFAULT_MAX_PAGE_SIZE_IN_BYTES / types.size());
            maxBlockSizeInBytes = Math.min(DEFAULT_MAX_BLOCK_SIZE_IN_BYTES, maxBlockSizeInBytes);
        }
        else {
            maxBlockSizeInBytes = 0;
        }
        pageBuilderStatus = new PageBuilderStatus(DEFAULT_MAX_PAGE_SIZE_IN_BYTES, maxBlockSizeInBytes);

        blockBuilders = new BlockBuilder[types.size()];
        for (int i = 0; i < blockBuilders.length; i++) {
            blockBuilders[i] = types.get(i).createBlockBuilder(pageBuilderStatus.createBlockBuilderStatus(), 1, pageBuilderStatus.getMaxBlockSizeInBytes());
        }
    }

    public void reset()
    {
        if (isEmpty()) {
            return;
        }
        declaredPositions = 0;
        pageBuilderStatus = new PageBuilderStatus(pageBuilderStatus.getMaxPageSizeInBytes(), pageBuilderStatus.getMaxBlockSizeInBytes());

        for (int i = 0; i < types.size(); i++) {
            blockBuilders[i] = types.get(i).createBlockBuilder(pageBuilderStatus.createBlockBuilderStatus(), 1, pageBuilderStatus.getMaxBlockSizeInBytes());
        }
    }

    public BlockBuilder getBlockBuilder(int channel)
    {
        return blockBuilders[channel];
    }

    /**
     * Hack to declare positions when producing a page with no channels
     */
    public void declarePosition()
    {
        declaredPositions++;
    }

    public boolean isFull()
    {
        return declaredPositions == Integer.MAX_VALUE || pageBuilderStatus.isFull();
    }

    public boolean isEmpty()
    {
        return declaredPositions == 0;
    }

    public int getPositionCount()
    {
        return declaredPositions;
    }

    public long getSizeInBytes()
    {
        return pageBuilderStatus.getSizeInBytes();
    }

    public Page build()
    {
        if (blockBuilders.length == 0) {
            return new Page(declaredPositions);
        }

        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = blockBuilders[i].build();
            if (blocks[i].getPositionCount() != declaredPositions) {
                throw new IllegalStateException(String.format("Declared positions (%s) does not match block %s's number of entries (%s)", declaredPositions, i, blocks[i].getPositionCount()));
            }
        }

        return new Page(blocks);
    }
}
