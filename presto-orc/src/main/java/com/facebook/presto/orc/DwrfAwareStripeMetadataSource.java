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
import com.facebook.presto.orc.metadata.DwrfStripeCache;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.orc.metadata.Stream.StreamArea.INDEX;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * This StripeMetadataSource implementation uses DwrfStripeCache to get stripe
 * footers and index streams if they are present in the cache, otherwise it
 * would read these pieces from the delegate StripeMetadataSource.
 */
public class DwrfAwareStripeMetadataSource
        implements StripeMetadataSource
{
    private final StripeMetadataSource delegate;
    private final DwrfStripeCache stripeCache;

    public DwrfAwareStripeMetadataSource(StripeMetadataSource delegate, DwrfStripeCache stripeCache)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.stripeCache = requireNonNull(stripeCache, "stripeCache is null");
    }

    @Override
    public Slice getStripeFooterSlice(OrcDataSource orcDataSource, StripeId stripeId, long footerOffset, int footerLength, boolean cacheable)
            throws IOException
    {
        Optional<Slice> stripeFooterSlice = stripeCache.getStripeFooterSlice(stripeId, footerLength);
        if (stripeFooterSlice.isPresent()) {
            return stripeFooterSlice.get();
        }
        return delegate.getStripeFooterSlice(orcDataSource, stripeId, footerOffset, footerLength, cacheable);
    }

    @Override
    public Map<StreamId, OrcDataSourceInput> getInputs(OrcDataSource orcDataSource, StripeId stripeId, Map<StreamId, DiskRange> diskRanges, boolean cacheable)
            throws IOException
    {
        Optional<Slice> stripeCacheIndexStreamsSlice = stripeCache.getIndexStreamsSlice(stripeId);
        if (!stripeCacheIndexStreamsSlice.isPresent()) {
            return delegate.getInputs(orcDataSource, stripeId, diskRanges, cacheable);
        }

        Slice cacheSlice = stripeCacheIndexStreamsSlice.get();
        ImmutableMap.Builder<StreamId, OrcDataSourceInput> inputsBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<StreamId, DiskRange> dataStreamsBuilder = ImmutableMap.builder();

        // read all index streams from the cache, and all data streams from the delegate
        for (Map.Entry<StreamId, DiskRange> entry : diskRanges.entrySet()) {
            StreamId streamId = entry.getKey();
            DiskRange diskRange = entry.getValue();
            if (streamId.getStreamKind().getStreamArea() == INDEX) {
                Slice slice = cacheSlice.slice(toIntExact(diskRange.getOffset()), diskRange.getLength());
                OrcDataSourceInput orcDataSourceInput = new OrcDataSourceInput(new BasicSliceInput(slice), slice.length());
                inputsBuilder.put(streamId, orcDataSourceInput);
            }
            else {
                dataStreamsBuilder.put(streamId, diskRange);
            }
        }

        ImmutableMap<StreamId, DiskRange> dataStreams = dataStreamsBuilder.build();
        if (!dataStreams.isEmpty()) {
            Map<StreamId, OrcDataSourceInput> dataStreamInputs = delegate.getInputs(orcDataSource, stripeId, dataStreams, cacheable);
            inputsBuilder.putAll(dataStreamInputs);
        }

        return inputsBuilder.build();
    }
}
