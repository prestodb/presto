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

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.TupleInfo;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class PageBuilder
{
    public static final DataSize DEFAULT_MAX_PAGE_SIZE = new DataSize(1, Unit.MEGABYTE);

    private final BlockBuilder[] blockBuilders;
    private final long maxSizeInBytes;
    private final int maxBlockSize;
    private int declaredPositions;

    public PageBuilder(List<TupleInfo> tupleInfos)
    {
        this(tupleInfos, DEFAULT_MAX_PAGE_SIZE);
    }

    public PageBuilder(List<TupleInfo> tupleInfos, DataSize maxSize)
    {
        if (!tupleInfos.isEmpty()) {
            maxBlockSize = (int) (maxSize.toBytes() / tupleInfos.size());
        }
        else {
            maxBlockSize = 0;
        }

        blockBuilders = new BlockBuilder[tupleInfos.size()];
        for (int i = 0; i < blockBuilders.length; i++) {
            blockBuilders[i] = new BlockBuilder(tupleInfos.get(i), maxBlockSize, new DynamicSliceOutput((int) (maxBlockSize * 1.5)));
        }
        this.maxSizeInBytes = checkNotNull(maxSize, "maxSize is null").toBytes();
    }

    public void reset()
    {
        declaredPositions = 0;
        if (isEmpty()) {
            return;
        }

        for (int i = 0; i < blockBuilders.length; i++) {
            BlockBuilder blockBuilder = blockBuilders[i];
            int estimatedSize = (int) (blockBuilder.size() * 1.5);
            blockBuilders[i] = new BlockBuilder(blockBuilder.getTupleInfo(), maxBlockSize, new DynamicSliceOutput(estimatedSize));
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
        if (declaredPositions == Integer.MAX_VALUE) {
            return true;
        }

        long sizeInBytes = 0;
        for (BlockBuilder blockBuilder : blockBuilders) {
            if (blockBuilder.isFull()) {
                return true;
            }
            sizeInBytes += blockBuilder.size();
            if (sizeInBytes > maxSizeInBytes) {
                return true;
            }
        }
        return false;
    }

    public boolean isEmpty()
    {
        return blockBuilders.length == 0 ? declaredPositions == 0 : blockBuilders[0].isEmpty();
    }

    public long getSize()
    {
        long sizeInBytes = 0;
        for (BlockBuilder blockBuilder : blockBuilders) {
            sizeInBytes += blockBuilder.size();
        }
        return sizeInBytes;
    }

    public Page build()
    {
        if (blockBuilders.length == 0) {
            return new Page(declaredPositions);
        }

        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = blockBuilders[i].build();
        }
        return new Page(blocks);
    }
}
