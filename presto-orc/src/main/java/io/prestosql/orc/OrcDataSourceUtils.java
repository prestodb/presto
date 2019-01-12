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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static java.lang.Math.toIntExact;

public final class OrcDataSourceUtils
{
    private OrcDataSourceUtils()
    {
    }

    /**
     * Merge disk ranges that are closer than {@code maxMergeDistance}.
     */
    public static List<DiskRange> mergeAdjacentDiskRanges(Collection<DiskRange> diskRanges, DataSize maxMergeDistance, DataSize maxReadSize)
    {
        // sort ranges by start offset
        List<DiskRange> ranges = new ArrayList<>(diskRanges);
        Collections.sort(ranges, new Comparator<DiskRange>()
        {
            @Override
            public int compare(DiskRange o1, DiskRange o2)
            {
                return Long.compare(o1.getOffset(), o2.getOffset());
            }
        });

        // merge overlapping ranges
        long maxReadSizeBytes = maxReadSize.toBytes();
        long maxMergeDistanceBytes = maxMergeDistance.toBytes();
        ImmutableList.Builder<DiskRange> result = ImmutableList.builder();
        DiskRange last = ranges.get(0);
        for (int i = 1; i < ranges.size(); i++) {
            DiskRange current = ranges.get(i);
            DiskRange merged = last.span(current);
            if (merged.getLength() <= maxReadSizeBytes && last.getEnd() + maxMergeDistanceBytes >= current.getOffset()) {
                last = merged;
            }
            else {
                result.add(last);
                last = current;
            }
        }
        result.add(last);

        return result.build();
    }

    /**
     * Get a slice for the disk range from the provided buffers.  The buffers ranges do not have
     * to exactly match {@code diskRange}, but {@code diskRange} must be completely contained within
     * one of the buffer ranges.
     */
    public static Slice getDiskRangeSlice(DiskRange diskRange, Map<DiskRange, byte[]> buffers)
    {
        for (Entry<DiskRange, byte[]> bufferEntry : buffers.entrySet()) {
            DiskRange bufferRange = bufferEntry.getKey();
            byte[] buffer = bufferEntry.getValue();
            if (bufferRange.contains(diskRange)) {
                int offset = toIntExact(diskRange.getOffset() - bufferRange.getOffset());
                return Slices.wrappedBuffer(buffer, offset, diskRange.getLength());
            }
        }
        throw new IllegalStateException("No matching buffer for disk range");
    }
}
