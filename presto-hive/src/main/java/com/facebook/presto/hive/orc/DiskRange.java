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
import com.google.common.primitives.Ints;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class DiskRange
        implements Comparable<DiskRange>
{
    private final long offset;
    private final int length;

    public DiskRange(long offset, int length)
    {
        checkArgument(offset >= 0, "offset is negative");
        checkArgument(length >= 0, "length is negative");

        this.offset = offset;
        this.length = length;
    }

    public long getOffset()
    {
        return offset;
    }

    public int getLength()
    {
        return length;
    }

    public long getEnd()
    {
        return offset + length;
    }

    public DiskRange mergeWith(DiskRange otherDiskRange)
    {
        checkNotNull(otherDiskRange, "otherDiskRange is null");
        long start = Math.min(this.offset, otherDiskRange.getOffset());
        long end = Math.max(getEnd(), otherDiskRange.getEnd());
        return new DiskRange(start, Ints.checkedCast(end - start));
    }

    public boolean isAdjacentTo(DiskRange other)
    {
        return (offset <= other.getEnd()) && (other.offset <= getEnd());
    }

    @Override
    public int compareTo(DiskRange other)
    {
        int result = Long.compare(offset, other.offset);
        if (result == 0) {
            // smaller ranges come before longer ranges
            result = Long.compare(length, other.length);
        }
        return result;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(offset, length);
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
        return Objects.equals(offset, other.offset) && Objects.equals(length, other.length);
    }

    @Override
    public String toString()
    {
        return com.google.common.base.Objects.toStringHelper(this)
                .add("offset", offset)
                .add("length", length)
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
