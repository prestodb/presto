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
import java.util.List;
import java.util.Objects;

/**
 * Defines the possible values of a single variable in terms of its valid scalar ranges and nullability.
 *
 * For example:
 *   Domain.none() => no scalar values allowed, NULL not allowed
 *   Domain.all() => all scalar values allowed, NULL allowed
 *   Domain.onlyNull() => no scalar values allowed, NULL allowed
 *   Domain.notNull() => all scalar values allowed, NULL not allowed
 */
public final class Domain
{
    private final SortedRangeSet ranges;
    private final boolean nullAllowed;

    @JsonCreator
    public Domain(
            @JsonProperty("ranges") SortedRangeSet ranges,
            @JsonProperty("nullAllowed") boolean nullAllowed)
    {
        this.ranges = Objects.requireNonNull(ranges, "ranges is null");
        this.nullAllowed = nullAllowed;
        if (ranges.getType().isPrimitive()) {
            throw new IllegalArgumentException("Primitive types not supported: " + ranges.getType());
        }
    }

    public static Domain create(SortedRangeSet ranges, boolean nullAllowed)
    {
        return new Domain(ranges, nullAllowed);
    }

    public static Domain none(Class<?> type)
    {
        return new Domain(SortedRangeSet.none(type), false);
    }

    public static Domain all(Class<?> type)
    {
        return new Domain(SortedRangeSet.of(Range.all(type)), true);
    }

    public static Domain onlyNull(Class<?> type)
    {
        return new Domain(SortedRangeSet.none(type), true);
    }

    public static Domain notNull(Class<?> type)
    {
        return new Domain(SortedRangeSet.all(type), false);
    }

    public static Domain singleValue(Comparable<?> value)
    {
        return new Domain(SortedRangeSet.of(Range.equal(value)), false);
    }

    @JsonIgnore
    public Class<?> getType()
    {
        return ranges.getType();
    }

    /**
     * Returns a SortedRangeSet to represent the set of scalar values that are allowed in this Domain.
     * An empty (a.k.a. "none") SortedRangeSet indicates that no scalar values are allowed.
     */
    @JsonProperty
    public SortedRangeSet getRanges()
    {
        return ranges;
    }

    @JsonProperty
    public boolean isNullAllowed()
    {
        return nullAllowed;
    }

    @JsonIgnore
    public boolean isNone()
    {
        return equals(Domain.none(getType()));
    }

    @JsonIgnore
    public boolean isAll()
    {
        return equals(Domain.all(getType()));
    }

    @JsonIgnore
    public boolean isSingleValue()
    {
        return !nullAllowed && ranges.isSingleValue();
    }

    @JsonIgnore
    public boolean isNullableSingleValue()
    {
        if (nullAllowed) {
            return ranges.isNone();
        }
        else {
            return ranges.isSingleValue();
        }
    }

    @JsonIgnore
    public boolean isOnlyNull()
    {
        return equals(onlyNull(getType()));
    }

    @JsonIgnore
    public Comparable<?> getSingleValue()
    {
        if (!isSingleValue()) {
            throw new IllegalStateException("Domain is not a single value");
        }
        return ranges.getSingleValue();
    }

    @JsonIgnore
    public Comparable<?> getNullableSingleValue()
    {
        if (!isNullableSingleValue()) {
            throw new IllegalStateException("Domain is not a single value");
        }

        if (nullAllowed) {
            return null;
        }
        else {
            return ranges.getSingleValue();
        }
    }

    public boolean includesValue(Comparable<?> value)
    {
        return value == null ? nullAllowed : ranges.includesMarker(Marker.exactly(value));
    }

    public boolean overlaps(Domain other)
    {
        checkTypeCompatibility(other);
        return !this.intersect(other).isNone();
    }

    public boolean contains(Domain other)
    {
        checkTypeCompatibility(other);
        return this.union(other).equals(this);
    }

    public Domain intersect(Domain other)
    {
        checkTypeCompatibility(other);
        SortedRangeSet intersectedRanges = this.getRanges().intersect(other.getRanges());
        boolean nullAllowed = this.isNullAllowed() && other.isNullAllowed();
        return new Domain(intersectedRanges, nullAllowed);
    }

    public Domain union(Domain other)
    {
        checkTypeCompatibility(other);
        SortedRangeSet unionRanges = this.getRanges().union(other.getRanges());
        boolean nullAllowed = this.isNullAllowed() || other.isNullAllowed();
        return new Domain(unionRanges, nullAllowed);
    }

    public static Domain union(List<Domain> domains)
    {
        if (domains.size() == 1) {
            return domains.get(0);
        }

        boolean nullAllowed = false;
        List<SortedRangeSet> ranges = new ArrayList<>();
        for (Domain domain : domains) {
            ranges.add(domain.getRanges());
            nullAllowed = nullAllowed || domain.nullAllowed;
        }

        return new Domain(SortedRangeSet.union(ranges), nullAllowed);
    }

    public Domain complement()
    {
        return new Domain(ranges.complement(), !nullAllowed);
    }

    public Domain subtract(Domain other)
    {
        checkTypeCompatibility(other);
        SortedRangeSet subtractedRanges = this.getRanges().subtract(other.getRanges());
        boolean nullAllowed = this.isNullAllowed() && !other.isNullAllowed();
        return new Domain(subtractedRanges, nullAllowed);
    }

    private void checkTypeCompatibility(Domain domain)
    {
        if (!getType().equals(domain.getType())) {
            throw new IllegalArgumentException(String.format("Mismatched Domain types: %s vs %s", getType(), domain.getType()));
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(ranges, nullAllowed);
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
        final Domain other = (Domain) obj;
        return Objects.equals(this.ranges, other.ranges) &&
                Objects.equals(this.nullAllowed, other.nullAllowed);
    }

    @Override
    public String toString()
    {
        List<Object> values = new ArrayList<>();
        if (nullAllowed) {
            values.add("NULL");
        }
        for (Range range : ranges) {
            values.add(range);
        }
        return values.toString();
    }
}
