/*
 * Copyright 2004-present Facebook. All Rights Reserved.
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
    private int declaredPositions;

    public PageBuilder(List<TupleInfo> tupleInfos)
    {
        this(tupleInfos, DEFAULT_MAX_PAGE_SIZE);
    }

    public PageBuilder(List<TupleInfo> tupleInfos, DataSize maxSize)
    {
        int bytesPerColumn = 0;
        if (!tupleInfos.isEmpty()) {
            bytesPerColumn = (int) (maxSize.toBytes() / tupleInfos.size());
        }

        blockBuilders = new BlockBuilder[tupleInfos.size()];
        for (int i = 0; i < blockBuilders.length; i++) {
            blockBuilders[i] = new BlockBuilder(tupleInfos.get(i), bytesPerColumn, new DynamicSliceOutput((int) (bytesPerColumn * 1.5)));
        }
        this.maxSizeInBytes = checkNotNull(maxSize, "maxSize is null").toBytes();
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
