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
import org.openjdk.jol.info.ClassLayout;

import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.statistics.DoubleRange.RANGE_SIZE;
import static com.facebook.presto.spi.statistics.Estimate.ESTIMATE_SIZE;
import static com.facebook.presto.spi.statistics.StringRange.STRING_RANGE_SIZE;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class ColumnStatistics
{
    private static final long COLUMN_STATISTICS_SIZE = ClassLayout.parseClass(ColumnStatistics.class).instanceSize();
    private static final long OPTION_SIZE = ClassLayout.parseClass(Optional.class).instanceSize();

    public static final double INFINITE_TO_FINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR = 0.25;
    public static final double INFINITE_TO_INFINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR = 0.5;
    private static final ColumnStatistics EMPTY = new ColumnStatistics(Estimate.unknown(), Estimate.unknown(), Estimate.unknown(), Optional.empty(), Optional.empty(), Optional.empty());
    private static final long STRING_CLASS_SIZE = ClassLayout.parseClass(String.class).instanceSize();

    private final Estimate nullsFraction;
    private final Estimate distinctValuesCount;
    private final Estimate dataSize;
    private final Optional<DoubleRange> range;
    private final Optional<StringRange> stringRange;
    private final Optional<ConnectorHistogram> histogram;

    public static ColumnStatistics empty()
    {
        return EMPTY;
    }

    @Deprecated
    public ColumnStatistics(
            Estimate nullsFraction,
            Estimate distinctValuesCount,
            Estimate dataSize,
            Optional<DoubleRange> range,
            Optional<ConnectorHistogram> histogram)
    {
        this(nullsFraction, distinctValuesCount, dataSize, range, Optional.empty(), histogram);
    }

    public ColumnStatistics(
            Estimate nullsFraction,
            Estimate distinctValuesCount,
            Estimate dataSize,
            Optional<DoubleRange> range,
            Optional<StringRange> stringRange,
            Optional<ConnectorHistogram> histogram)
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
        this.stringRange = requireNonNull(stringRange, "string range is null");
        this.histogram = requireNonNull(histogram, "histogram is null");
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

    @JsonProperty
    public Optional<StringRange> getStringRange()
    {
        return stringRange;
    }

    @JsonProperty
    public Optional<ConnectorHistogram> getHistogram()
    {
        return histogram;
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
                Objects.equals(range, that.range) &&
                Objects.equals(stringRange, that.stringRange) &&
                Objects.equals(histogram, that.histogram);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nullsFraction, distinctValuesCount, dataSize, range, stringRange, histogram);
    }

    @Override
    public String toString()
    {
        return "ColumnStatistics{" +
                "nullsFraction=" + nullsFraction +
                ", distinctValuesCount=" + distinctValuesCount +
                ", dataSize=" + dataSize +
                ", range=" + range +
                ", stringRange=" + stringRange +
                ", histogram=" + histogram +
                '}';
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder buildFrom(ColumnStatistics statistics)
    {
        return new Builder()
                .setRange(statistics.getRange())
                .setDataSize(statistics.getDataSize())
                .setNullsFraction(statistics.getNullsFraction())
                .setDistinctValuesCount(statistics.getDistinctValuesCount())
                .setHistogram(statistics.getHistogram())
                .setStringRange(statistics.getStringRange());
    }

    public long getEstimatedSize()
    {
        return COLUMN_STATISTICS_SIZE +
                3 * ESTIMATE_SIZE +
                estimateHistogram(histogram) +
                estimateRange(range) +
                estimateStringRange(stringRange);
    }

    private long estimateRange(Optional<DoubleRange> range)
    {
        return OPTION_SIZE + range.map(unused -> RANGE_SIZE).orElse(0L);
    }

    private long estimateHistogram(Optional<ConnectorHistogram> histogram)
    {
        return OPTION_SIZE + histogram.map(ConnectorHistogram::getEstimatedSize).orElse(0L);
    }

    private long estimateStringRange(Optional<StringRange> stringRange)
    {
        return stringRange.map(value -> {
            // An empty string occupies 16 bytes in JVM, adding 16 * 2 to account for min and max.
            long estimate = OPTION_SIZE + STRING_RANGE_SIZE + 32 + value.getMin().getBytes(UTF_8).length + value.getMax().getBytes(UTF_8).length;
            return (long) (1.1 * estimate); // Overestimate by 10% since in-memory size is not the same as UTF-8 serialized size
        }).orElse(OPTION_SIZE);
    }

    /**
     * If one of the estimates below is unspecified, the default "unknown" estimate value
     * (represented by floating point NaN) may cause the resulting symbol statistics
     * to be "unknown" as well.
     *
     * @see VariableStatsEstimate
     */
    public static final class Builder
    {
        private Estimate nullsFraction = Estimate.unknown();
        private Estimate distinctValuesCount = Estimate.unknown();
        private Estimate dataSize = Estimate.unknown();
        private Optional<DoubleRange> range = Optional.empty();
        private Optional<StringRange> stringRange = Optional.empty();
        private Optional<ConnectorHistogram> histogram = Optional.empty();

        public Builder setNullsFraction(Estimate nullsFraction)
        {
            this.nullsFraction = requireNonNull(nullsFraction, "nullsFraction is null");
            return this;
        }

        public Estimate getNullsFraction()
        {
            return nullsFraction;
        }

        public Builder setDistinctValuesCount(Estimate distinctValuesCount)
        {
            this.distinctValuesCount = requireNonNull(distinctValuesCount, "distinctValuesCount is null");
            return this;
        }

        public Estimate getDistinctValuesCount()
        {
            return distinctValuesCount;
        }

        public Builder setDataSize(Estimate dataSize)
        {
            this.dataSize = requireNonNull(dataSize, "dataSize is null");
            return this;
        }

        public Estimate getDataSize()
        {
            return dataSize;
        }

        public Builder setRange(DoubleRange range)
        {
            this.range = Optional.of(requireNonNull(range, "range is null"));
            return this;
        }

        public Builder setStringRange(StringRange stringRange)
        {
            this.stringRange = Optional.of(requireNonNull(stringRange, "stringRange is null"));
            return this;
        }

        public Builder setStringRange(Optional<StringRange> stringRange)
        {
            this.stringRange = requireNonNull(stringRange, "stringRange is null");
            return this;
        }

        public Builder setRange(Optional<DoubleRange> range)
        {
            this.range = requireNonNull(range, "range is null");
            return this;
        }

        public Builder setHistogram(Optional<ConnectorHistogram> histogram)
        {
            this.histogram = histogram;
            return this;
        }

        public Optional<ConnectorHistogram> getHistogram()
        {
            return histogram;
        }

        public Builder mergeWith(Builder other)
        {
            if (nullsFraction.isUnknown()) {
                this.nullsFraction = other.nullsFraction;
            }

            if (distinctValuesCount.isUnknown()) {
                this.distinctValuesCount = other.distinctValuesCount;
            }

            if (dataSize.isUnknown()) {
                this.dataSize = other.dataSize;
            }

            if (!range.isPresent()) {
                this.range = other.range;
            }

            if (!stringRange.isPresent()) {
                this.stringRange = other.stringRange;
            }

            if (!histogram.isPresent()) {
                this.histogram = other.histogram;
            }

            return this;
        }

        private void validate()
        {
            if (stringRange.isPresent() && range.isPresent()) {
                throw new IllegalArgumentException("Both StringRange and Range cannot be defined simultaneously on a column.");
            }
        }

        public ColumnStatistics build()
        {
            validate();
            return new ColumnStatistics(nullsFraction, distinctValuesCount, dataSize, range, stringRange, histogram);
        }
    }
}
