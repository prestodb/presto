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
package com.facebook.presto.orc;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.orc.StripeReader.StripeId;
import com.facebook.presto.orc.StripeReader.StripeStreamId;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;
import com.facebook.presto.orc.stream.OrcInputStream;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.facebook.presto.common.RuntimeUnit.BYTE;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.BLOOM_FILTER;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_INDEX;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class CachingStripeMetadataSource
        implements StripeMetadataSource
{
    private final StripeMetadataSource delegate;
    private final Cache<StripeId, CacheableSlice> footerSliceCache;
    private final Cache<StripeStreamId, CacheableSlice> stripeStreamCache;
    private final Optional<Cache<StripeStreamId, CacheableRowGroupIndices>> rowGroupIndexCache;

    public CachingStripeMetadataSource(StripeMetadataSource delegate, Cache<StripeId, CacheableSlice> footerSliceCache, Cache<StripeStreamId, CacheableSlice> stripeStreamCache, Optional<Cache<StripeStreamId, CacheableRowGroupIndices>> rowGroupIndexCache)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.footerSliceCache = requireNonNull(footerSliceCache, "footerSliceCache is null");
        this.stripeStreamCache = requireNonNull(stripeStreamCache, "rowIndexSliceCache is null");
        this.rowGroupIndexCache = requireNonNull(rowGroupIndexCache, "rowGroupIndexCache is null");
    }

    @Override
    public Slice getStripeFooterSlice(OrcDataSource orcDataSource, StripeId stripeId, long footerOffset, int footerLength, boolean cacheable, long fileModificationTime)
            throws IOException
    {
        if (!cacheable) {
            return delegate.getStripeFooterSlice(orcDataSource, stripeId, footerOffset, footerLength, cacheable, fileModificationTime);
        }
        try {
            CacheableSlice cacheableSlice = footerSliceCache.getIfPresent(stripeId);
            if (cacheableSlice != null) {
                if (cacheableSlice.getFileModificationTime() == fileModificationTime) {
                    return cacheableSlice.getSlice();
                }
                footerSliceCache.invalidate(stripeId);
                // This get call is to increment the miss count for invalidated entries so the stats are recorded correctly.
                footerSliceCache.getIfPresent(stripeId);
            }
            cacheableSlice = new CacheableSlice(delegate.getStripeFooterSlice(orcDataSource, stripeId, footerOffset, footerLength, cacheable, fileModificationTime), fileModificationTime);
            footerSliceCache.put(stripeId, cacheableSlice);
            return cacheableSlice.getSlice();
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), IOException.class);
            throw new IOException("Unexpected error in stripe footer reading after footerSliceCache miss", e.getCause());
        }
    }

    @Override
    public Map<StreamId, OrcDataSourceInput> getInputs(OrcDataSource orcDataSource, StripeId stripeId, Map<StreamId, DiskRange> diskRanges, boolean cacheable, long fileModificationTime)
            throws IOException
    {
        if (!cacheable) {
            return delegate.getInputs(orcDataSource, stripeId, diskRanges, cacheable, fileModificationTime);
        }

        // Fetch existing stream slice from cache
        ImmutableMap.Builder<StreamId, OrcDataSourceInput> inputsBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<StreamId, DiskRange> uncachedDiskRangesBuilder = ImmutableMap.builder();
        for (Entry<StreamId, DiskRange> entry : diskRanges.entrySet()) {
            StripeStreamId stripeStreamId = new StripeStreamId(stripeId, entry.getKey());
            if (isCachedStream(entry.getKey().getStreamKind())) {
                CacheableSlice streamSlice = stripeStreamCache.getIfPresent(stripeStreamId);
                if (streamSlice != null && streamSlice.getFileModificationTime() == fileModificationTime) {
                    inputsBuilder.put(entry.getKey(), new OrcDataSourceInput(new BasicSliceInput(streamSlice.getSlice()), streamSlice.getSlice().length()));
                }
                else {
                    if (streamSlice != null) {
                        stripeStreamCache.invalidate(stripeStreamId);
                        // This get call is to increment the miss count for invalidated entries so the stats are recorded correctly.
                        stripeStreamCache.getIfPresent(stripeStreamId);
                    }
                    uncachedDiskRangesBuilder.put(entry);
                }
            }
            else {
                uncachedDiskRangesBuilder.put(entry);
            }
        }

        // read ranges and update cache
        Map<StreamId, OrcDataSourceInput> uncachedInputs = delegate.getInputs(orcDataSource, stripeId, uncachedDiskRangesBuilder.build(), cacheable, fileModificationTime);
        for (Entry<StreamId, OrcDataSourceInput> entry : uncachedInputs.entrySet()) {
            if (isCachedStream(entry.getKey().getStreamKind())) {
                // We need to rewind the input after eagerly reading the slice.
                Slice streamSlice = Slices.wrappedBuffer(entry.getValue().getInput().readSlice(toIntExact(entry.getValue().getInput().length())).getBytes());
                stripeStreamCache.put(new StripeStreamId(stripeId, entry.getKey()), new CacheableSlice(streamSlice, fileModificationTime));
                inputsBuilder.put(entry.getKey(), new OrcDataSourceInput(new BasicSliceInput(streamSlice), toIntExact(streamSlice.getRetainedSize())));
            }
            else {
                inputsBuilder.put(entry.getKey(), entry.getValue());
            }
        }
        return inputsBuilder.build();
    }

    @Override
    public List<RowGroupIndex> getRowIndexes(
            MetadataReader metadataReader,
            HiveWriterVersion hiveWriterVersion,
            StripeId stripId,
            StreamId streamId,
            OrcInputStream inputStream,
            List<HiveBloomFilter> bloomFilters,
            RuntimeStats runtimeStats,
            long fileModificationTime)
            throws IOException
    {
        StripeStreamId stripeStreamId = new StripeStreamId(stripId, streamId);
        if (rowGroupIndexCache.isPresent()) {
            CacheableRowGroupIndices cacheableRowGroupIndices = rowGroupIndexCache.get().getIfPresent(stripeStreamId);
            if (cacheableRowGroupIndices != null && cacheableRowGroupIndices.getFileModificationTime() == fileModificationTime) {
                runtimeStats.addMetricValue("OrcRowGroupIndexCacheHit", NONE, 1);
                runtimeStats.addMetricValue("OrcRowGroupIndexInMemoryBytesRead", BYTE, cacheableRowGroupIndices.getRowGroupIndices().stream().mapToLong(RowGroupIndex::getRetainedSizeInBytes).sum());
                return cacheableRowGroupIndices.getRowGroupIndices();
            }
            else {
                if (cacheableRowGroupIndices != null) {
                    rowGroupIndexCache.get().invalidate(stripeStreamId);
                    // This get call is to increment the miss count for invalidated entries so the stats are recorded correctly.
                    rowGroupIndexCache.get().getIfPresent(stripeStreamId);
                }
                runtimeStats.addMetricValue("OrcRowGroupIndexCacheHit", NONE, 0);
                runtimeStats.addMetricValue("OrcRowGroupIndexStorageBytesRead", BYTE, inputStream.getRetainedSizeInBytes());
            }
        }
        List<RowGroupIndex> rowGroupIndices = delegate.getRowIndexes(metadataReader, hiveWriterVersion, stripId, streamId, inputStream, bloomFilters, runtimeStats, fileModificationTime);
        if (rowGroupIndexCache.isPresent()) {
            rowGroupIndexCache.get().put(stripeStreamId, new CacheableRowGroupIndices(rowGroupIndices, fileModificationTime));
        }
        return rowGroupIndices;
    }

    private static boolean isCachedStream(StreamKind streamKind)
    {
        // BLOOM_FILTER and ROW_INDEX are on the critical path to generate a stripe. Other stream kinds could be lazily read.
        return streamKind == BLOOM_FILTER || streamKind == ROW_INDEX;
    }
}
