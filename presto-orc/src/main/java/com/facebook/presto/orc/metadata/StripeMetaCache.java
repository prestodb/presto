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
package com.facebook.presto.orc.metadata;

import com.facebook.airlift.log.Logger;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.orc.metadata.StripeMetaCacheMode.FOOTER;
import static com.facebook.presto.orc.metadata.StripeMetaCacheMode.INDEX;
import static com.facebook.presto.orc.metadata.StripeMetaCacheMode.INDEX_AND_FOOTER;
import static com.facebook.presto.orc.metadata.StripeMetaCacheMode.NONE;
import static java.util.Objects.requireNonNull;

public class StripeMetaCache
{
    private static final Logger log = Logger.get(StripeMetaCache.class);
    private final List<Integer> cacheOffsets;
    private final StripeMetaCacheMode cacheMode;
    private final Slice data;

    public StripeMetaCache(List<Integer> offsets, StripeMetaCacheMode mode, Slice data)
    {
        this.cacheOffsets = ImmutableList.copyOf(requireNonNull(offsets, "offsets is null"));
        this.data = requireNonNull(data, "data is null");
        this.cacheMode = filterMode(requireNonNull(mode, "mode is null"));
    }

    // work only with supported modes
    private StripeMetaCacheMode filterMode(StripeMetaCacheMode mode)
    {
        if (mode == INDEX || mode == FOOTER || mode == INDEX_AND_FOOTER) {
            return mode;
        }
        return NONE;
    }

    public Optional<Slice> getStripeFooterSlice(int stripeOrdinal, long sliceLength)
    {
        return getSlice(stripeOrdinal, FOOTER, sliceLength);
    }

    public Optional<Slice> getStripeIndexSlice(int stripeOrdinal, long expectedSliceLength)
    {
        return getSlice(stripeOrdinal, INDEX, expectedSliceLength);
    }

    private Optional<Slice> getSlice(int stripeOrdinal, StripeMetaCacheMode kind, long expectedSliceLength)
    {
        if (!cacheMode.has(kind)) {
            return Optional.empty();
        }

        int index = -1;
        if (cacheMode == kind) {
            index = stripeOrdinal;
        }
        else if (cacheMode == INDEX_AND_FOOTER) {
            index = stripeOrdinal * 2 + (kind == INDEX ? 0 : 1);
        }

        // cache might contain data only for first N stripes
        if (index >= 0 && index < cacheOffsets.size() - 1) {
            int sliceOffsetStart = cacheOffsets.get(index);
            int sliceOffsetEnd = cacheOffsets.get(index + 1);
            int dataLength = sliceOffsetEnd - sliceOffsetStart;
            if (dataLength != expectedSliceLength) {
                log.warn("Size mismatch between expected %s and cached data %s for stripe %s", expectedSliceLength, dataLength, stripeOrdinal);
                return Optional.empty();
            }

            if (data.length() < sliceOffsetEnd || data.length() < sliceOffsetStart) {
                log.warn("Cached data size is smaller than to what offsets point to");
                return Optional.empty();
            }

            return Optional.of(data.slice(sliceOffsetStart, dataLength));
        }

        return Optional.empty();
    }
}
