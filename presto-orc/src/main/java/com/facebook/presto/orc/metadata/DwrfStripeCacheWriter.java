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

import com.facebook.presto.common.io.DataOutput;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Accumulates data for DWRF stripe cache using given mode and max size.
 */
public class DwrfStripeCacheWriter
{
    private final DwrfStripeCacheMode mode;
    private final DynamicSliceOutput cache;
    private final long maxSize;
    private final List<Integer> offsets = new ArrayList<>();

    private int size;
    private boolean full;

    public DwrfStripeCacheWriter(DwrfStripeCacheMode mode, DataSize maxSize)
    {
        // make sure max size is in the int range
        this.maxSize = toIntExact(requireNonNull(maxSize, "maxSize is null").toBytes());
        this.mode = requireNonNull(mode, "mode is null");
        this.cache = new DynamicSliceOutput(64);
        if (mode == DwrfStripeCacheMode.NONE) {
            full = true;
        }
    }

    public void addIndexStreams(List<DataOutput> indexStreams, long indexSize)
    {
        if (full || !mode.hasIndex()) {
            return;
        }

        if (size + indexSize > maxSize) {
            full = true;
            return;
        }

        indexStreams.forEach(indexStream -> indexStream.writeData(cache));
        incrementSize(indexSize);
    }

    public void addStripeFooter(DataOutput stripeFooter)
    {
        if (full || !mode.hasFooter()) {
            return;
        }

        long stripeFooterSize = stripeFooter.size();
        if (size + stripeFooterSize > maxSize) {
            full = true;
            return;
        }

        stripeFooter.writeData(cache);
        incrementSize(stripeFooterSize);
    }

    private void incrementSize(long sizeIncrement)
    {
        if (offsets.isEmpty()) {
            offsets.add(0);
        }
        this.size += sizeIncrement;
        offsets.add(this.size);
    }

    public DwrfStripeCacheData getDwrfStripeCacheData()
    {
        return new DwrfStripeCacheData(cache.slice(), size, mode);
    }

    public List<Integer> getOffsets()
    {
        return ImmutableList.copyOf(offsets);
    }
}
