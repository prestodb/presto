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
package com.facebook.presto.spi.statistics;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static com.facebook.presto.spi.statistics.Estimate.unknownValue;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public final class ColumnStatistics
{
    private static final List<RangeColumnStatistics> SINGLE_UNKNOWN_RANGE_STATISTICS = singletonList(RangeColumnStatistics.builder().build());

    private final Estimate nullsFraction;
    private final List<RangeColumnStatistics> rangeColumnStatistics;

    private ColumnStatistics(Estimate nullsFraction, List<RangeColumnStatistics> rangeColumnStatistics)
    {
        this.nullsFraction = requireNonNull(nullsFraction, "nullsFraction can not be null");
        requireNonNull(rangeColumnStatistics, "rangeColumnStatistics can not be null");
        if (!rangeColumnStatistics.stream().allMatch(Objects::nonNull)) {
            throw new NullPointerException("elements of rangeColumnStatistics can not be null");
        }
        if (rangeColumnStatistics.size() > 1) {
            // todo add support for multiple ranges.
            throw new IllegalArgumentException("Statistics for multiple ranges are not supported");
        }
        if (rangeColumnStatistics.isEmpty()) {
            rangeColumnStatistics = SINGLE_UNKNOWN_RANGE_STATISTICS;
        }
        if (nullsFraction.isValueUnknown() != rangeColumnStatistics.get(0).getFraction().isValueUnknown()) {
            throw new IllegalArgumentException("All or none fraction/nullsFraction must be set");
        }

        this.rangeColumnStatistics = unmodifiableList(new ArrayList<>(rangeColumnStatistics));
    }

    public Estimate getNullsFraction()
    {
        return nullsFraction;
    }

    public RangeColumnStatistics getOnlyRangeColumnStatistics()
    {
        return rangeColumnStatistics.get(0);
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
        ColumnStatistics that = (ColumnStatistics) o;
        return Objects.equals(nullsFraction, that.nullsFraction) &&
                Objects.equals(rangeColumnStatistics, that.rangeColumnStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nullsFraction, rangeColumnStatistics);
    }

    @Override
    public String toString()
    {
        return "ColumnStatistics{" +
                "nullsFraction=" + nullsFraction +
                ", rangeColumnStatistics=" + rangeColumnStatistics +
                '}';
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Estimate nullsFraction = unknownValue();
        private List<RangeColumnStatistics> rangeColumnStatistics = new ArrayList<>();

        public Builder setNullsFraction(Estimate nullsFraction)
        {
            this.nullsFraction = nullsFraction;
            return this;
        }

        public Builder addRange(Consumer<RangeColumnStatistics.Builder> rangeBuilderConsumer)
        {
            RangeColumnStatistics.Builder rangeBuilder = RangeColumnStatistics.builder();
            rangeBuilderConsumer.accept(rangeBuilder);
            addRange(rangeBuilder.build());
            return this;
        }

        public Builder addRange(RangeColumnStatistics rangeColumnStatistics)
        {
            this.rangeColumnStatistics.add(rangeColumnStatistics);
            return this;
        }

        public ColumnStatistics build()
        {
            return new ColumnStatistics(nullsFraction, rangeColumnStatistics);
        }
    }
}
