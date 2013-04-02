/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.tpch;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.serde.BlocksFileReader;
import com.facebook.presto.serde.BlocksFileStats;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
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
    public BlockIterable  getBlocks(TpchTableHandle tableHandle, TpchColumnHandle columnHandle, BlocksFileEncoding encoding)
    {
        Slice slice = getColumnSlice(tableHandle, columnHandle, encoding);
        BlocksFileReader blocks = BlocksFileReader.readBlocks(slice);
        statsBuilder.add(blocks.getStats());
        return blocks;
    }

    @Override
    public DataSize getColumnDataSize(TpchTableHandle tableHandle, TpchColumnHandle columnHandle, BlocksFileEncoding encoding)
    {
        Slice slice = getColumnSlice(tableHandle, columnHandle, encoding);
        return new DataSize(slice.length(), Unit.BYTE);
    }

    private Slice getColumnSlice(TpchTableHandle tableHandle, TpchColumnHandle columnHandle, BlocksFileEncoding encoding)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");
        checkNotNull(encoding, "encoding is null");

        File columnFile = tpchDataProvider.getDataFile(tableHandle, columnHandle, encoding);
        return mappedFileCache.getUnchecked(columnFile.getAbsolutePath());
    }

    public List<BlocksFileStats> getStats()
    {
        return statsBuilder.build();
    }
}
