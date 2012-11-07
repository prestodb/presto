/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.tpch;

import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.serde.BlocksFileReader;
import com.facebook.presto.serde.BlocksFileStats;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.tpch.TpchSchema.Column;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.io.File;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class StatsTpchBlocksProvider
        implements TpchBlocksProvider
{
    private static final LoadingCache<String, Slice> mappedFileCache = CacheBuilder.newBuilder().build(new CacheLoader<String, Slice>(){
        @Override
        public Slice load(String key)
                throws Exception
        {
            File file = new File(key);
            Slice slice = Slices.mapFileReadOnly(file);
            return slice;
        }
    });

    private final TpchDataProvider tpchDataProvider;
    private final ImmutableList.Builder<BlocksFileStats> statsBuilder = ImmutableList.builder();

    public StatsTpchBlocksProvider(TpchDataProvider tpchDataProvider)
    {
        this.tpchDataProvider = checkNotNull(tpchDataProvider, "tpchDataProvider is null");
    }

    @Override
    public BlocksFileReader getBlocks(Column column, BlocksFileEncoding encoding)
    {
        Slice slice = getColumnSlice(column, encoding);
        BlocksFileReader blocks = BlocksFileReader.readBlocks(slice);
        statsBuilder.add(blocks.getStats());
        return blocks;
    }

    @Override
    public DataSize getColumnDataSize(Column column, BlocksFileEncoding encoding)
    {
        Slice slice = getColumnSlice(column, encoding);
        return new DataSize(slice.length(), Unit.BYTE);
    }

    private Slice getColumnSlice(Column column, BlocksFileEncoding encoding)
    {
        checkNotNull(column, "column is null");
        checkNotNull(encoding, "encoding is null");

        File columnFile = tpchDataProvider.getColumnFile(column, encoding);
        return mappedFileCache.getUnchecked(columnFile.getAbsolutePath());
    }

    public List<BlocksFileStats> getStats()
    {
        return statsBuilder.build();
    }
}
