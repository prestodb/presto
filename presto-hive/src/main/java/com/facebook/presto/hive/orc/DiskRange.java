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
package com.facebook.presto.hive.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class DiskRange
        implements Comparable<DiskRange>
{
    private final long offset;
    private final long end;

    public DiskRange(long offset, long end)
    {
        checkArgument(end >= offset, "invalid range");
        this.offset = offset;
        this.end = end;
    }

    public long getOffset()
    {
        return offset;
    }

    public long getLength()
    {
        return end - offset;
    }

    public long getEnd()
    {
        return end;
    }

    public DiskRange mergeWith(DiskRange otherDiskRange)
    {
        checkNotNull(otherDiskRange, "otherDiskRange is null");
        return new DiskRange(Math.min(offset, otherDiskRange.getOffset()), Math.max(end, otherDiskRange.getEnd()));
    }

    public boolean isAdjacentTo(DiskRange other)
    {
        return (offset <= other.end) && (other.offset <= end);
    }

    @Override
    public int compareTo(DiskRange other)
    {
        int result = Long.compare(offset, other.offset);
        if (result == 0) {
            result = Long.compare(end, other.end);
        }
        return result;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(offset, end);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DiskRange other = (DiskRange) obj;
        return Objects.equals(offset, other.offset) && Objects.equals(end, other.end);
    }

    @Override
    public String toString()
    {
        return com.google.common.base.Objects.toStringHelper(this)
                .add("offset", offset)
                .add("end", end)
                .toString();
    }

    public static List<DiskRange> mergeAdjacentDiskRanges(Iterable<DiskRange> diskRanges)
    {
        checkNotNull(diskRanges, "diskRanges is null");

        List<DiskRange> sortedDiskRanges = Ordering.natural().sortedCopy(diskRanges);

        if (sortedDiskRanges.isEmpty()) {
            return sortedDiskRanges;
        }

        ImmutableList.Builder<DiskRange> builder = ImmutableList.builder();
        DiskRange previous = sortedDiskRanges.get(0);
        for (int i = 1; i < sortedDiskRanges.size(); i++) {
            DiskRange current = sortedDiskRanges.get(i);
            if (previous.isAdjacentTo(current)) {
                previous = previous.mergeWith(current);
            }
            else {
                builder.add(previous);
                previous = current;
            }
        }
        builder.add(previous);
        return builder.build();
    }
}
