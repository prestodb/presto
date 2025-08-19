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
package com.facebook.presto.hive.metastore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class HiveColumnStatistics
{
    private static final HiveColumnStatistics EMPTY = HiveColumnStatistics.builder().build();

    private final Optional<IntegerStatistics> integerStatistics;
    private final Optional<DoubleStatistics> doubleStatistics;
    private final Optional<DecimalStatistics> decimalStatistics;
    private final Optional<DateStatistics> dateStatistics;
    private final Optional<BooleanStatistics> booleanStatistics;
    private final OptionalLong maxValueSizeInBytes;
    private final OptionalLong totalSizeInBytes;
    private final OptionalLong nullsCount;
    private final OptionalLong distinctValuesCount;

    public static HiveColumnStatistics empty()
    {
        return EMPTY;
    }

    @JsonCreator
    public HiveColumnStatistics(
            @JsonProperty("integerStatistics") Optional<IntegerStatistics> integerStatistics,
            @JsonProperty("doubleStatistics") Optional<DoubleStatistics> doubleStatistics,
            @JsonProperty("decimalStatistics") Optional<DecimalStatistics> decimalStatistics,
            @JsonProperty("dateStatistics") Optional<DateStatistics> dateStatistics,
            @JsonProperty("booleanStatistics") Optional<BooleanStatistics> booleanStatistics,
            @JsonProperty("maxValueSizeInBytes") OptionalLong maxValueSizeInBytes,
            @JsonProperty("totalSizeInBytes") OptionalLong totalSizeInBytes,
            @JsonProperty("nullsCount") OptionalLong nullsCount,
            @JsonProperty("distinctValuesCount") OptionalLong distinctValuesCount)
    {
        this.integerStatistics = requireNonNull(integerStatistics, "integerStatistics is null");
        this.doubleStatistics = requireNonNull(doubleStatistics, "doubleStatistics is null");
        this.decimalStatistics = requireNonNull(decimalStatistics, "decimalStatistics is null");
        this.dateStatistics = requireNonNull(dateStatistics, "dateStatistics is null");
        this.booleanStatistics = requireNonNull(booleanStatistics, "booleanStatistics is null");
        this.maxValueSizeInBytes = requireNonNull(maxValueSizeInBytes, "maxValueSizeInBytes is null");
        this.totalSizeInBytes = requireNonNull(totalSizeInBytes, "totalSizeInBytes is null");
        this.nullsCount = requireNonNull(nullsCount, "nullsCount is null");
        this.distinctValuesCount = requireNonNull(distinctValuesCount, "distinctValuesCount is null");

        List<String> presentStatistics = new ArrayList<>();
        integerStatistics.ifPresent(s -> presentStatistics.add("integerStatistics"));
        doubleStatistics.ifPresent(s -> presentStatistics.add("doubleStatistics"));
        decimalStatistics.ifPresent(s -> presentStatistics.add("decimalStatistics"));
        dateStatistics.ifPresent(s -> presentStatistics.add("dateStatistics"));
        booleanStatistics.ifPresent(s -> presentStatistics.add("booleanStatistics"));
        checkArgument(presentStatistics.size() <= 1, "multiple type specific statistic objects are present: %s", presentStatistics);
    }

    @JsonProperty
    public Optional<IntegerStatistics> getIntegerStatistics()
    {
        return integerStatistics;
    }

    @JsonProperty
    public Optional<DoubleStatistics> getDoubleStatistics()
    {
        return doubleStatistics;
    }

    @JsonProperty
    public Optional<DecimalStatistics> getDecimalStatistics()
    {
        return decimalStatistics;
    }

    @JsonProperty
    public Optional<DateStatistics> getDateStatistics()
    {
        return dateStatistics;
    }

    @JsonProperty
    public Optional<BooleanStatistics> getBooleanStatistics()
    {
        return booleanStatistics;
    }

    @JsonProperty
    public OptionalLong getMaxValueSizeInBytes()
    {
        return maxValueSizeInBytes;
    }

    @JsonProperty
    public OptionalLong getTotalSizeInBytes()
    {
        return totalSizeInBytes;
    }

    @JsonProperty
    public OptionalLong getNullsCount()
    {
        return nullsCount;
    }

    @JsonProperty
    public OptionalLong getDistinctValuesCount()
    {
        return distinctValuesCount;
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
        HiveColumnStatistics that = (HiveColumnStatistics) o;
        return Objects.equals(integerStatistics, that.integerStatistics) &&
                Objects.equals(doubleStatistics, that.doubleStatistics) &&
                Objects.equals(decimalStatistics, that.decimalStatistics) &&
                Objects.equals(dateStatistics, that.dateStatistics) &&
                Objects.equals(booleanStatistics, that.booleanStatistics) &&
                Objects.equals(maxValueSizeInBytes, that.maxValueSizeInBytes) &&
                Objects.equals(totalSizeInBytes, that.totalSizeInBytes) &&
                Objects.equals(nullsCount, that.nullsCount) &&
                Objects.equals(distinctValuesCount, that.distinctValuesCount);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                integerStatistics,
                doubleStatistics,
                decimalStatistics,
                dateStatistics,
                booleanStatistics,
                maxValueSizeInBytes,
                totalSizeInBytes,
                nullsCount,
                distinctValuesCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("integerStatistics", integerStatistics)
                .add("doubleStatistics", doubleStatistics)
                .add("decimalStatistics", decimalStatistics)
                .add("dateStatistics", dateStatistics)
                .add("booleanStatistics", booleanStatistics)
                .add("maxValueSizeInBytes", maxValueSizeInBytes)
                .add("totalSizeInBytes", totalSizeInBytes)
                .add("nullsCount", nullsCount)
                .add("distinctValuesCount", distinctValuesCount)
                .toString();
    }

    public static HiveColumnStatistics createIntegerColumnStatistics(OptionalLong min, OptionalLong max, OptionalLong nullsCount, OptionalLong distinctValuesCount)
    {
        return builder()
                .setIntegerStatistics(new IntegerStatistics(min, max))
                .setNullsCount(nullsCount)
                .setDistinctValuesCount(distinctValuesCount)
                .build();
    }

    public static HiveColumnStatistics createDoubleColumnStatistics(OptionalDouble min, OptionalDouble max, OptionalLong nullsCount, OptionalLong distinctValuesCount)
    {
        return builder()
                .setDoubleStatistics(new DoubleStatistics(min, max))
                .setNullsCount(nullsCount)
                .setDistinctValuesCount(distinctValuesCount)
                .build();
    }

    public static HiveColumnStatistics createDecimalColumnStatistics(Optional<BigDecimal> min, Optional<BigDecimal> max, OptionalLong nullsCount, OptionalLong distinctValuesCount)
    {
        return builder()
                .setDecimalStatistics(new DecimalStatistics(min, max))
                .setNullsCount(nullsCount)
                .setDistinctValuesCount(distinctValuesCount)
                .build();
    }

    public static HiveColumnStatistics createDateColumnStatistics(Optional<LocalDate> min, Optional<LocalDate> max, OptionalLong nullsCount, OptionalLong distinctValuesCount)
    {
        return builder()
                .setDateStatistics(new DateStatistics(min, max))
                .setNullsCount(nullsCount)
                .setDistinctValuesCount(distinctValuesCount)
                .build();
    }

    public static HiveColumnStatistics createBooleanColumnStatistics(OptionalLong trueCount, OptionalLong falseCount, OptionalLong nullsCount)
    {
        return builder()
                .setBooleanStatistics(new BooleanStatistics(trueCount, falseCount))
                .setNullsCount(nullsCount)
                .build();
    }

    public static HiveColumnStatistics createStringColumnStatistics(
            OptionalLong maxValueSizeInBytes,
            OptionalLong totalSizeInBytes,
            OptionalLong nullsCount,
            OptionalLong distinctValuesCount)
    {
        return builder()
                .setMaxValueSizeInBytes(maxValueSizeInBytes)
                .setTotalSizeInBytes(totalSizeInBytes)
                .setNullsCount(nullsCount)
                .setDistinctValuesCount(distinctValuesCount)
                .build();
    }

    public static HiveColumnStatistics createBinaryColumnStatistics(OptionalLong maxValueSizeInBytes, OptionalLong totalSizeInBytes, OptionalLong nullsCount)
    {
        return builder()
                .setMaxValueSizeInBytes(maxValueSizeInBytes)
                .setTotalSizeInBytes(totalSizeInBytes)
                .setNullsCount(nullsCount)
                .build();
    }

    public static Builder builder(HiveColumnStatistics other)
    {
        return new Builder(other);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Optional<IntegerStatistics> integerStatistics = Optional.empty();
        private Optional<DoubleStatistics> doubleStatistics = Optional.empty();
        private Optional<DecimalStatistics> decimalStatistics = Optional.empty();
        private Optional<DateStatistics> dateStatistics = Optional.empty();
        private Optional<BooleanStatistics> booleanStatistics = Optional.empty();
        private OptionalLong maxValueSizeInBytes = OptionalLong.empty();
        private OptionalLong totalSizeInBytes = OptionalLong.empty();
        private OptionalLong nullsCount = OptionalLong.empty();
        private OptionalLong distinctValuesCount = OptionalLong.empty();

        private Builder() {}

        private Builder(HiveColumnStatistics other)
        {
            this.integerStatistics = other.getIntegerStatistics();
            this.doubleStatistics = other.getDoubleStatistics();
            this.decimalStatistics = other.getDecimalStatistics();
            this.dateStatistics = other.getDateStatistics();
            this.booleanStatistics = other.getBooleanStatistics();
            this.maxValueSizeInBytes = other.getMaxValueSizeInBytes();
            this.totalSizeInBytes = other.getTotalSizeInBytes();
            this.nullsCount = other.getNullsCount();
            this.distinctValuesCount = other.getDistinctValuesCount();
        }

        public Builder setIntegerStatistics(Optional<IntegerStatistics> integerStatistics)
        {
            this.integerStatistics = integerStatistics;
            return this;
        }

        public Builder setIntegerStatistics(IntegerStatistics integerStatistics)
        {
            this.integerStatistics = Optional.of(integerStatistics);
            return this;
        }

        public Builder setDoubleStatistics(Optional<DoubleStatistics> doubleStatistics)
        {
            this.doubleStatistics = doubleStatistics;
            return this;
        }

        public Builder setDoubleStatistics(DoubleStatistics doubleStatistics)
        {
            this.doubleStatistics = Optional.of(doubleStatistics);
            return this;
        }

        public Builder setDecimalStatistics(Optional<DecimalStatistics> decimalStatistics)
        {
            this.decimalStatistics = decimalStatistics;
            return this;
        }

        public Builder setDecimalStatistics(DecimalStatistics decimalStatistics)
        {
            this.decimalStatistics = Optional.of(decimalStatistics);
            return this;
        }

        public Builder setDateStatistics(Optional<DateStatistics> dateStatistics)
        {
            this.dateStatistics = dateStatistics;
            return this;
        }

        public Builder setDateStatistics(DateStatistics dateStatistics)
        {
            this.dateStatistics = Optional.of(dateStatistics);
            return this;
        }

        public Builder setBooleanStatistics(Optional<BooleanStatistics> booleanStatistics)
        {
            this.booleanStatistics = booleanStatistics;
            return this;
        }

        public Builder setBooleanStatistics(BooleanStatistics booleanStatistics)
        {
            this.booleanStatistics = Optional.of(booleanStatistics);
            return this;
        }

        public Builder setMaxValueSizeInBytes(long maxValueSizeInBytes)
        {
            this.maxValueSizeInBytes = OptionalLong.of(maxValueSizeInBytes);
            return this;
        }

        public Builder setMaxValueSizeInBytes(OptionalLong maxValueSizeInBytes)
        {
            this.maxValueSizeInBytes = maxValueSizeInBytes;
            return this;
        }

        public Builder setTotalSizeInBytes(long totalSizeInBytes)
        {
            this.totalSizeInBytes = OptionalLong.of(totalSizeInBytes);
            return this;
        }

        public Builder setTotalSizeInBytes(OptionalLong totalSizeInBytes)
        {
            this.totalSizeInBytes = totalSizeInBytes;
            return this;
        }

        public Builder setNullsCount(OptionalLong nullsCount)
        {
            this.nullsCount = nullsCount;
            return this;
        }

        public Builder setNullsCount(long nullsCount)
        {
            this.nullsCount = OptionalLong.of(nullsCount);
            return this;
        }

        public Builder setDistinctValuesCount(OptionalLong distinctValuesCount)
        {
            this.distinctValuesCount = distinctValuesCount;
            return this;
        }

        public Builder setDistinctValuesCount(long distinctValuesCount)
        {
            this.distinctValuesCount = OptionalLong.of(distinctValuesCount);
            return this;
        }

        public HiveColumnStatistics build()
        {
            return new HiveColumnStatistics(
                    integerStatistics,
                    doubleStatistics,
                    decimalStatistics,
                    dateStatistics,
                    booleanStatistics,
                    maxValueSizeInBytes,
                    totalSizeInBytes,
                    nullsCount,
                    distinctValuesCount);
        }
    }
}
