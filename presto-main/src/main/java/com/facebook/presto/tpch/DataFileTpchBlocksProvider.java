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
package com.facebook.presto.tpch;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.serde.BlocksFileReader;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.File;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DataFileTpchBlocksProvider
        implements TpchBlocksProvider
{
    private static final LoadingCache<String, Slice> mappedFileCache = CacheBuilder.newBuilder().build(new CacheLoader<String, Slice>()
    {
        @Override
        public Slice load(String key)
                throws Exception
        {
            File file = new File(key);
            Slice slice = Slices.mapFileReadOnly(file);
            return slice;
        }
    });

    private final TpchDataFileLoader tpchDataFileLoader;

    public DataFileTpchBlocksProvider(TpchDataFileLoader tpchDataFileLoader)
    {
        this.tpchDataFileLoader = checkNotNull(tpchDataFileLoader, "tpchDataProvider is null");
    }

    @Override
    public BlockIterable getBlocks(TpchTableHandle tableHandle,
            TpchColumnHandle columnHandle,
            int partNumber,
            int totalParts,
            BlocksFileEncoding encoding)
    {
        checkArgument(totalParts > 0, "totalParts must be > 1");
        checkArgument(partNumber >= 0, "partNumber must be >= 0");

        Slice slice = getColumnSlice(tableHandle, columnHandle, encoding);
        return BlocksFileReader.readBlocks(slice);
    }

    private Slice getColumnSlice(TpchTableHandle tableHandle, TpchColumnHandle columnHandle, BlocksFileEncoding encoding)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");
        checkNotNull(encoding, "encoding is null");

        File columnFile = tpchDataFileLoader.getDataFile(tableHandle, columnHandle, encoding);
        return mappedFileCache.getUnchecked(columnFile.getAbsolutePath());
    }
}
