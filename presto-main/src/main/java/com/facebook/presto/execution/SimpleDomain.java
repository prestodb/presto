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
package com.facebook.presto.execution;

import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Range;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public final class SimpleDomain
{
    private final boolean nullAllowed;
    private final Optional<List<SimpleRange>> ranges;

    @JsonCreator
    public SimpleDomain(
            @JsonProperty("nullAllowed") boolean nullAllowed,
            @JsonProperty("ranges") Optional<List<SimpleRange>> ranges)
    {
        this.nullAllowed = nullAllowed;
        checkNotNull(ranges, "ranges is null");
        List<SimpleRange> rangesCopy = null;
        if (ranges.isPresent()) {
            rangesCopy = ImmutableList.copyOf(ranges.get());
        }
        this.ranges = Optional.ofNullable(rangesCopy);
    }

    @JsonProperty
    public boolean isNullAllowed()
    {
        return nullAllowed;
    }

    @JsonProperty
    public Optional<List<SimpleRange>> getRanges()
    {
        return ranges;
    }

    private static List<SimpleRange> getSimpleRangeList(Domain domain)
    {
        checkNotNull(domain, "domain is null");
        if (domain.isAll()) {
            return null;
        }
        if (domain.isNone()) {
            return ImmutableList.<SimpleRange>of();
        }
        ImmutableList.Builder<SimpleRange> rangeBuilder = ImmutableList.builder();
        for (Range range : domain.getRanges()) {
            rangeBuilder.add(SimpleRange.fromRange(range));
        }
        return rangeBuilder.build();
    }

    public static SimpleDomain fromDomain(Domain domain)
    {
        if (domain == null) {
            return null;
        }
        return new SimpleDomain(domain.isNullAllowed(), Optional.of(getSimpleRangeList(domain)));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SimpleDomain that = (SimpleDomain) o;

        return Objects.equals(this.nullAllowed, that.nullAllowed) &&
                Objects.equals(this.ranges, that.ranges);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nullAllowed, ranges);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(nullAllowed)
                .addValue(ranges)
                .toString();
    }
}
