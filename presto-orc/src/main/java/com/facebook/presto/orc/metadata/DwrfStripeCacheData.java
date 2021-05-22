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

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;

public class DwrfStripeCacheData
{
    private final Slice cacheSlice;
    private final int cacheSliceSize;
    private final DwrfStripeCacheMode mode;

    public DwrfStripeCacheData(Slice stripeCacheSlice, int stripeCacheSize, DwrfStripeCacheMode stripeCacheMode)
    {
        this.cacheSlice = requireNonNull(stripeCacheSlice, "stripeCacheSlice is null");
        this.cacheSliceSize = stripeCacheSize;
        this.mode = requireNonNull(stripeCacheMode, "stripeCacheMode is null");
    }

    public Slice getDwrfStripeCacheSlice()
    {
        return cacheSlice;
    }

    public int getDwrfStripeCacheSize()
    {
        return cacheSliceSize;
    }

    public DwrfStripeCacheMode getDwrfStripeCacheMode()
    {
        return mode;
    }

    public DwrfStripeCache buildDwrfStripeCache(List<StripeInformation> stripes, List<Integer> cacheOffsets)
    {
        // AbstractOrcRecordReader sorts stripes by position, so probably there
        // is a use-case when stripes can come unordered
        List<StripeInformation> sortedStripes = new ArrayList<>(stripes);
        sortedStripes.sort(comparingLong(StripeInformation::getOffset));

        ImmutableMap.Builder<Long, Slice> indexSlices = ImmutableMap.builder();
        if (mode.hasIndex()) {
            for (int i = 0; i < sortedStripes.size(); i++) {
                int offsetIdx = mode.getIndexOffsetPosition(i);
                if (!isValidCacheOffset(offsetIdx, cacheOffsets)) {
                    break;
                }
                addSlice(offsetIdx, cacheOffsets, indexSlices, sortedStripes.get(i));
            }
        }

        ImmutableMap.Builder<Long, Slice> footerSlices = ImmutableMap.builder();
        if (mode.hasFooter()) {
            for (int i = 0; i < sortedStripes.size(); i++) {
                int offsetIdx = mode.getFooterOffsetPosition(i);
                if (!isValidCacheOffset(offsetIdx, cacheOffsets)) {
                    break;
                }
                addSlice(offsetIdx, cacheOffsets, footerSlices, sortedStripes.get(i));
            }
        }

        return new DwrfStripeCache(mode, indexSlices.build(), footerSlices.build());
    }

    private boolean isValidCacheOffset(int cacheOffsetIdx, List<Integer> cacheOffsets)
    {
        return (cacheOffsetIdx + 1) < cacheOffsets.size();
    }

    private void addSlice(int cacheOffsetIdx, List<Integer> cacheOffsets, ImmutableMap.Builder<Long, Slice> slices, StripeInformation stripe)
    {
        int sliceOffset = cacheOffsets.get(cacheOffsetIdx);
        int sliceSize = cacheOffsets.get(cacheOffsetIdx + 1) - sliceOffset;
        checkState(sliceOffset < cacheSlice.length(),
                "stripe cache offset %s is out of bound for cache slice of size %s",
                sliceOffset,
                cacheSlice.length());
        checkState(sliceOffset + sliceSize <= cacheSlice.length(),
                "stripe cache offset+size=%s is out of bound for cache slice of size %s",
                sliceOffset + sliceSize,
                cacheSlice.length());
        slices.put(stripe.getOffset(), cacheSlice.slice(sliceOffset, sliceSize));
    }
}
