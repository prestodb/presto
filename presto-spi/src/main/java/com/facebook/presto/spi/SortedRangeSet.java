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
package com.facebook.presto.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

import static java.util.Collections.unmodifiableCollection;

/**
 * A set containing zero or more Ranges of the same type over a continuous space of possible values.
 * Ranges are coalesced into the most compact representation of non-overlapping Ranges. This structure
 * allows iteration across these compacted Ranges in increasing order, as well as other common
 * set-related operation.
 */
public final class SortedRangeSet
        implements Iterable<Range>
{
    private final Class<?> type;
    private final NavigableMap<Marker, Range> lowIndexedRanges;

    private SortedRangeSet(Class<?> type, NavigableMap<Marker, Range> lowIndexedRanges)
    {
        this.type = Objects.requireNonNull(type, "type is null");
        this.lowIndexedRanges = Objects.requireNonNull(lowIndexedRanges, "lowIndexedRanges is null");
    }

    public static SortedRangeSet none(Class<?> type)
    {
        return copyOf(type, Collections.<Range>emptyList());
    }

    public static SortedRangeSet all(Class<?> type)
    {
        return copyOf(type, Arrays.asList(Range.all(type)));
    }

    public static SortedRangeSet singleValue(Comparable<?> value)
    {
        return SortedRangeSet.of(Range.equal(value));
    }

    /**
     * Provided Ranges are unioned together to form the SortedRangeSet
     */
    public static SortedRangeSet of(Range first, Range... ranges)
    {
        List<Range> rangeList = new ArrayList<>();
        rangeList.add(first);
        rangeList.addAll(Arrays.asList(ranges));
        return copyOf(first.getType(), rangeList);
    }

    /**
     * Provided Ranges are unioned together to form the SortedRangeSet
     */
    public static SortedRangeSet copyOf(Class<?> type, Iterable<Range> ranges)
    {
        return new Builder(type).addAll(ranges).build();
    }

    @JsonCreator
    public static SortedRangeSet copyOf(
            @JsonProperty("type") Class<?> type,
            @JsonProperty("ranges") List<Range> ranges)
    {
        return copyOf(type, (Iterable<Range>) ranges);
    }

    @JsonProperty
    public Class<?> getType()
    {
        return type;
    }

    @JsonProperty
    public List<Range> getRanges()
    {
        ArrayList<Range> ranges = new ArrayList<>();
        ranges.addAll(lowIndexedRanges.values());
        return ranges;
    }

    @JsonIgnore
    public int getRangeCount()
    {
        return lowIndexedRanges.size();
    }

    @JsonIgnore
    public boolean isNone()
    {
        return lowIndexedRanges.isEmpty();
    }

    @JsonIgnore
    public boolean isAll()
    {
        return lowIndexedRanges.size() == 1 && lowIndexedRanges.values().iterator().next().isAll();
    }

    @JsonIgnore
    public boolean isSingleValue()
    {
        return lowIndexedRanges.size() == 1 && lowIndexedRanges.values().iterator().next().isSingleValue();
    }

    @JsonIgnore
    public Comparable<?> getSingleValue()
    {
        if (!isSingleValue()) {
            throw new IllegalStateException("SortedRangeSet does not have just a single value");
        }
        return lowIndexedRanges.values().iterator().next().getSingleValue();
    }

    public boolean includesMarker(Marker marker)
    {
        Objects.requireNonNull(marker, "marker is null");
        checkTypeCompatibility(marker);
        Map.Entry<Marker, Range> floorEntry = lowIndexedRanges.floorEntry(marker);
        return floorEntry != null && floorEntry.getValue().includes(marker);
    }

    @JsonIgnore
    public Range getSpan()
    {
        if (lowIndexedRanges.isEmpty()) {
            throw new IllegalStateException("Can not get span if no ranges exist");
        }
        return lowIndexedRanges.firstEntry().getValue().span(lowIndexedRanges.lastEntry().getValue());
    }

    public boolean overlaps(SortedRangeSet other)
    {
        checkTypeCompatibility(other);
        return !this.intersect(other).isNone();
    }

    public boolean contains(SortedRangeSet other)
    {
        checkTypeCompatibility(other);
        return this.union(other).equals(this);
    }

    public SortedRangeSet intersect(SortedRangeSet other)
    {
        checkTypeCompatibility(other);

        Builder builder = new Builder(type);

        Iterator<Range> iter1 = iterator();
        Iterator<Range> iter2 = other.iterator();

        if (iter1.hasNext() && iter2.hasNext()) {
            Range range1 = iter1.next();
            Range range2 = iter2.next();

            while (true) {
                if (range1.overlaps(range2)) {
                    builder.add(range1.intersect(range2));
                }

                if (range1.getHigh().compareTo(range2.getHigh()) <= 0) {
                    if (!iter1.hasNext()) {
                        break;
                    }
                    range1 = iter1.next();
                }
                else {
                    if (!iter2.hasNext()) {
                        break;
                    }
                    range2 = iter2.next();
                }
            }
        }

        return builder.build();
    }

    public SortedRangeSet union(SortedRangeSet other)
    {
        checkTypeCompatibility(other);
        return new Builder(type)
                .addAll(this)
                .addAll(other)
                .build();
    }

    public SortedRangeSet complement()
    {
        Builder builder = new Builder(type);

        if (lowIndexedRanges.isEmpty()) {
            return builder.add(Range.all(type)).build();
        }

        Iterator<Range> rangeIterator = lowIndexedRanges.values().iterator();

        Range firstRange = rangeIterator.next();
        if (!firstRange.getLow().isLowerUnbounded()) {
            builder.add(new Range(Marker.lowerUnbounded(type), firstRange.getLow().lesserAdjacent()));
        }

        Range previousRange = firstRange;
        while (rangeIterator.hasNext()) {
            Range currentRange = rangeIterator.next();

            Marker lowMarker = previousRange.getHigh().greaterAdjacent();
            Marker highMarker = currentRange.getLow().lesserAdjacent();
            builder.add(new Range(lowMarker, highMarker));

            previousRange = currentRange;
        }

        Range lastRange = previousRange;
        if (!lastRange.getHigh().isUpperUnbounded()) {
            builder.add(new Range(lastRange.getHigh().greaterAdjacent(), Marker.upperUnbounded(type)));
        }

        return builder.build();
    }

    public SortedRangeSet subtract(SortedRangeSet other)
    {
        checkTypeCompatibility(other);
        return this.intersect(other.complement());
    }

    private void checkTypeCompatibility(SortedRangeSet other)
    {
        if (!getType().equals(other.getType())) {
            throw new IllegalStateException(String.format("Mismatched SortedRangeSet types: %s vs %s", getType(), other.getType()));
        }
    }

    private void checkTypeCompatibility(Marker marker)
    {
        if (!getType().equals(marker.getType())) {
            throw new IllegalStateException(String.format("Marker of %s does not match SortedRangeSet of %s", marker.getType(), getType()));
        }
    }

    @Override
    public Iterator<Range> iterator()
    {
        return unmodifiableCollection(lowIndexedRanges.values()).iterator();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lowIndexedRanges);
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
        final SortedRangeSet other = (SortedRangeSet) obj;
        return Objects.equals(this.lowIndexedRanges, other.lowIndexedRanges);
    }

    @Override
    public String toString()
    {
        return lowIndexedRanges.values().toString();
    }

    public static Builder builder(Class<?> type)
    {
        return new Builder(type);
    }

    public static class Builder
    {
        private final Class<?> type;
        private final NavigableMap<Marker, Range> lowIndexedRanges = new TreeMap<>();

        public Builder(Class<?> type)
        {
            this.type = Objects.requireNonNull(type, "type is null");
        }

        public Builder add(Range range)
        {
            if (!type.equals(range.getType())) {
                throw new IllegalArgumentException(String.format("Range type %s does not match builder type %s", range.getType(), type));
            }

            // Merge with any overlapping ranges
            Map.Entry<Marker, Range> lowFloorEntry = lowIndexedRanges.floorEntry(range.getLow());
            if (lowFloorEntry != null && lowFloorEntry.getValue().overlaps(range)) {
                range = lowFloorEntry.getValue().span(range);
            }
            Map.Entry<Marker, Range> highFloorEntry = lowIndexedRanges.floorEntry(range.getHigh());
            if (highFloorEntry != null && highFloorEntry.getValue().overlaps(range)) {
                range = highFloorEntry.getValue().span(range);
            }

            // Merge with any adjacent ranges
            if (lowFloorEntry != null && lowFloorEntry.getValue().getHigh().isAdjacent(range.getLow())) {
                range = lowFloorEntry.getValue().span(range);
            }
            Map.Entry<Marker, Range> highHigherEntry = lowIndexedRanges.higherEntry(range.getHigh());
            if (highHigherEntry != null && highHigherEntry.getValue().getLow().isAdjacent(range.getHigh())) {
                range = highHigherEntry.getValue().span(range);
            }

            // Delete all encompassed ranges
            NavigableMap<Marker, Range> subMap = lowIndexedRanges.subMap(range.getLow(), true, range.getHigh(), true);
            subMap.clear();

            lowIndexedRanges.put(range.getLow(), range);
            return this;
        }

        public Builder addAll(Iterable<Range> ranges)
        {
            for (Range range : ranges) {
                add(range);
            }
            return this;
        }

        public SortedRangeSet build()
        {
            return new SortedRangeSet(type, lowIndexedRanges);
        }
    }
}
