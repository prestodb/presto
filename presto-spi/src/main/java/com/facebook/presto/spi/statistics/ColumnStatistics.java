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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class ColumnStatistics
{
    private static final ColumnStatistics EMPTY = new ColumnStatistics(Estimate.unknown(), Estimate.unknown(), Estimate.unknown(), Optional.empty());

    private final Estimate nullsFraction;
    private final Estimate distinctValuesCount;
    private final Estimate dataSize;
    private final Optional<DoubleRange> range;

    public static ColumnStatistics empty()
    {
        return EMPTY;
    }

    public ColumnStatistics(
            Estimate nullsFraction,
            Estimate distinctValuesCount,
            Estimate dataSize,
            Optional<DoubleRange> range)
    {
        this.nullsFraction = requireNonNull(nullsFraction, "nullsFraction is null");
        if (!nullsFraction.isUnknown()) {
            if (nullsFraction.getValue() < 0 || nullsFraction.getValue() > 1) {
                throw new IllegalArgumentException(format("nullsFraction must be between 0 and 1: %s", nullsFraction.getValue()));
            }
        }
        this.distinctValuesCount = requireNonNull(distinctValuesCount, "distinctValuesCount is null");
        if (!distinctValuesCount.isUnknown() && distinctValuesCount.getValue() < 0) {
            throw new IllegalArgumentException(format("distinctValuesCount must be greater than or equal to 0: %s", distinctValuesCount.getValue()));
        }
        this.dataSize = requireNonNull(dataSize, "dataSize is null");
        if (!dataSize.isUnknown() && dataSize.getValue() < 0) {
            throw new IllegalArgumentException(format("dataSize must be greater than or equal to 0: %s", dataSize.getValue()));
        }
        this.range = requireNonNull(range, "range is null");
    }

    @JsonProperty
    public Estimate getNullsFraction()
    {
        return nullsFraction;
    }

    @JsonProperty
    public Estimate getDistinctValuesCount()
    {
        return distinctValuesCount;
    }

    @JsonProperty
    public Estimate getDataSize()
    {
        return dataSize;
    }

    @JsonProperty
    public Optional<DoubleRange> getRange()
    {
        return range;
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
                Objects.equals(distinctValuesCount, that.distinctValuesCount) &&
                Objects.equals(dataSize, that.dataSize) &&
                Objects.equals(range, that.range);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nullsFraction, distinctValuesCount, dataSize, range);
    }

    @Override
    public String toString()
    {
        return "ColumnStatistics{" +
                "nullsFraction=" + nullsFraction +
                ", distinctValuesCount=" + distinctValuesCount +
                ", dataSize=" + dataSize +
                ", range=" + range +
                '}';
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * If one of the estimates below is unspecified, the default "unknown" estimate value
     * (represented by floating point NaN) may cause the resulting symbol statistics
     * to be "unknown" as well.
     * @see SymbolStatsEstimate
     */
    public static final class Builder
    {
        private Estimate nullsFraction = Estimate.unknown();
        private Estimate distinctValuesCount = Estimate.unknown();
        private Estimate dataSize = Estimate.unknown();
        private Optional<DoubleRange> range = Optional.empty();

        public Builder setNullsFraction(Estimate nullsFraction)
        {
            this.nullsFraction = requireNonNull(nullsFraction, "nullsFraction is null");
            return this;
        }

        public Builder setDistinctValuesCount(Estimate distinctValuesCount)
        {
            this.distinctValuesCount = requireNonNull(distinctValuesCount, "distinctValuesCount is null");
            return this;
        }

        public Builder setDataSize(Estimate dataSize)
        {
            this.dataSize = requireNonNull(dataSize, "dataSize is null");
            return this;
        }

        public Builder setRange(DoubleRange range)
        {
            this.range = Optional.of(requireNonNull(range, "range is null"));
            return this;
        }

        public Builder setRange(Optional<DoubleRange> range)
        {
            this.range = requireNonNull(range, "range is null");
            return this;
        }

        public ColumnStatistics build()
        {
            return new ColumnStatistics(nullsFraction, distinctValuesCount, dataSize, range);
        }
    }
}
