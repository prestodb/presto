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

import com.facebook.presto.orc.StripeReader.StripeId;
import com.facebook.presto.orc.StripeReader.StripeStreamId;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.BLOOM_FILTER;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_INDEX;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class CachingStripeMetadataSource
        implements StripeMetadataSource
{
    private final StripeMetadataSource delegate;
    private final Cache<StripeId, Slice> footerSliceCache;
    private final Cache<StripeStreamId, Slice> stripeStreamCache;

    public CachingStripeMetadataSource(StripeMetadataSource delegate, Cache<StripeId, Slice> footerSliceCache, Cache<StripeStreamId, Slice> stripeStreamCache)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.footerSliceCache = requireNonNull(footerSliceCache, "footerSliceCache is null");
        this.stripeStreamCache = requireNonNull(stripeStreamCache, "rowIndexSliceCache is null");
    }

    @Override
    public Slice getStripeFooterSlice(OrcDataSource orcDataSource, StripeId stripeId, long footerOffset, int footerLength, boolean cacheable)
            throws IOException
    {
        try {
            if (!cacheable) {
                return delegate.getStripeFooterSlice(orcDataSource, stripeId, footerOffset, footerLength, cacheable);
            }
            return footerSliceCache.get(stripeId, () -> delegate.getStripeFooterSlice(orcDataSource, stripeId, footerOffset, footerLength, cacheable));
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), IOException.class);
            throw new IOException("Unexpected error in stripe footer reading after footerSliceCache miss", e.getCause());
        }
    }

    @Override
    public Map<StreamId, OrcDataSourceInput> getInputs(OrcDataSource orcDataSource, StripeId stripeId, Map<StreamId, DiskRange> diskRanges, boolean cacheable)
            throws IOException
    {
        if (!cacheable) {
            return delegate.getInputs(orcDataSource, stripeId, diskRanges, cacheable);
        }

        // Fetch existing stream slice from cache
        ImmutableMap.Builder<StreamId, OrcDataSourceInput> inputsBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<StreamId, DiskRange> uncachedDiskRangesBuilder = ImmutableMap.builder();
        for (Entry<StreamId, DiskRange> entry : diskRanges.entrySet()) {
            if (isCachedStream(entry.getKey().getStreamKind())) {
                Slice streamSlice = stripeStreamCache.getIfPresent(new StripeStreamId(stripeId, entry.getKey()));
                if (streamSlice != null) {
                    inputsBuilder.put(entry.getKey(), new OrcDataSourceInput(new BasicSliceInput(streamSlice), streamSlice.length()));
                }
                else {
                    uncachedDiskRangesBuilder.put(entry);
                }
            }
            else {
                uncachedDiskRangesBuilder.put(entry);
            }
        }

        // read ranges and update cache
        Map<StreamId, OrcDataSourceInput> uncachedInputs = delegate.getInputs(orcDataSource, stripeId, uncachedDiskRangesBuilder.build(), cacheable);
        for (Entry<StreamId, OrcDataSourceInput> entry : uncachedInputs.entrySet()) {
            if (isCachedStream(entry.getKey().getStreamKind())) {
                // We need to rewind the input after eagerly reading the slice.
                Slice streamSlice = Slices.wrappedBuffer(entry.getValue().getInput().readSlice(toIntExact(entry.getValue().getInput().length())).getBytes());
                stripeStreamCache.put(new StripeStreamId(stripeId, entry.getKey()), streamSlice);
                inputsBuilder.put(entry.getKey(), new OrcDataSourceInput(new BasicSliceInput(streamSlice), toIntExact(streamSlice.getRetainedSize())));
            }
            else {
                inputsBuilder.put(entry.getKey(), entry.getValue());
            }
        }
        return inputsBuilder.build();
    }

    private static boolean isCachedStream(StreamKind streamKind)
    {
        // BLOOM_FILTER and ROW_INDEX are on the critical path to generate a stripe. Other stream kinds could be lazily read.
        return streamKind == BLOOM_FILTER || streamKind == ROW_INDEX;
    }
}
