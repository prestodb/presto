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
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static com.fasterxml.jackson.annotation.JsonTypeInfo.Id.NAME;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HiveColumnStatistics
{
    private final Optional<?> lowValue;
    private final Optional<?> highValue;
    private final OptionalLong maxColumnLength;
    private final OptionalDouble averageColumnLength;
    private final OptionalLong trueCount;
    private final OptionalLong falseCount;
    private final OptionalLong nullsCount;
    private final OptionalLong distinctValuesCount;

    @JsonCreator
    public HiveColumnStatistics(
            @JsonProperty("lowValue") Optional<?> lowValue,
            @JsonProperty("highValue") Optional<?> highValue,
            @JsonProperty("maxColumnLength") OptionalLong maxColumnLength,
            @JsonProperty("averageColumnLength") OptionalDouble averageColumnLength,
            @JsonProperty("trueCount") OptionalLong trueCount,
            @JsonProperty("falseCount") OptionalLong falseCount,
            @JsonProperty("nullsCount") OptionalLong nullsCount,
            @JsonProperty("distinctValuesCount") OptionalLong distinctValuesCount)
    {
        this.lowValue = requireNonNull(lowValue, "lowValue is null");
        this.highValue = requireNonNull(highValue, "highValue is null");
        this.maxColumnLength = requireNonNull(maxColumnLength, "maxColumnLength is null");
        this.averageColumnLength = requireNonNull(averageColumnLength, "averageColumnLength is null");
        this.trueCount = requireNonNull(trueCount, "trueCount is null");
        this.falseCount = requireNonNull(falseCount, "falseCount is null");
        this.nullsCount = requireNonNull(nullsCount, "nullsCount is null");
        this.distinctValuesCount = requireNonNull(distinctValuesCount, "distinctValuesCount is null");
    }

    @JsonProperty
    @JsonTypeInfo(use = NAME, property = "@lowValueClass")
    public Optional<?> getLowValue()
    {
        return lowValue;
    }

    @JsonProperty
    @JsonTypeInfo(use = NAME, property = "@highValueClass")
    public Optional<?> getHighValue()
    {
        return highValue;
    }

    @JsonProperty
    public OptionalLong getMaxColumnLength()
    {
        return maxColumnLength;
    }

    @JsonProperty
    public OptionalDouble getAverageColumnLength()
    {
        return averageColumnLength;
    }

    @JsonProperty
    public OptionalLong getTrueCount()
    {
        return trueCount;
    }

    @JsonProperty
    public OptionalLong getFalseCount()
    {
        return falseCount;
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
        return Objects.equals(lowValue, that.lowValue) &&
                Objects.equals(highValue, that.highValue) &&
                Objects.equals(maxColumnLength, that.maxColumnLength) &&
                Objects.equals(averageColumnLength, that.averageColumnLength) &&
                Objects.equals(trueCount, that.trueCount) &&
                Objects.equals(falseCount, that.falseCount) &&
                Objects.equals(nullsCount, that.nullsCount) &&
                Objects.equals(distinctValuesCount, that.distinctValuesCount);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                lowValue,
                highValue,
                maxColumnLength,
                averageColumnLength,
                trueCount,
                falseCount,
                nullsCount,
                distinctValuesCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("lowValue", lowValue)
                .add("highValue", highValue)
                .add("maxColumnLength", maxColumnLength)
                .add("averageColumnLength", averageColumnLength)
                .add("trueCount", trueCount)
                .add("falseCount", falseCount)
                .add("nullsCount", nullsCount)
                .add("distinctValuesCount", distinctValuesCount)
                .toString();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Optional<Comparable<?>> lowValue = Optional.empty();
        private Optional<Comparable<?>> highValue = Optional.empty();
        private OptionalLong maxColumnLength = OptionalLong.empty();
        private OptionalDouble averageColumnLength = OptionalDouble.empty();
        private OptionalLong trueCount = OptionalLong.empty();
        private OptionalLong falseCount = OptionalLong.empty();
        private OptionalLong nullsCount = OptionalLong.empty();
        private OptionalLong distinctValuesCount = OptionalLong.empty();

        public Builder setLowValue(Comparable<?> lowValue)
        {
            this.lowValue = Optional.of(lowValue);
            return this;
        }

        public Builder setHighValue(Comparable<?> highValue)
        {
            this.highValue = Optional.of(highValue);
            return this;
        }

        public Builder setMaxColumnLength(long maxColumnLength)
        {
            this.maxColumnLength = OptionalLong.of(maxColumnLength);
            return this;
        }

        public Builder setAverageColumnLength(double averageColumnLength)
        {
            this.averageColumnLength = OptionalDouble.of(averageColumnLength);
            return this;
        }

        public Builder setTrueCount(long trueCount)
        {
            this.trueCount = OptionalLong.of(trueCount);
            return this;
        }

        public Builder setFalseCount(long falseCount)
        {
            this.falseCount = OptionalLong.of(falseCount);
            return this;
        }

        public Builder setNullsCount(long nullsCount)
        {
            this.nullsCount = OptionalLong.of(nullsCount);
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
                    lowValue,
                    highValue,
                    maxColumnLength,
                    averageColumnLength,
                    trueCount,
                    falseCount,
                    nullsCount,
                    distinctValuesCount);
        }
    }
}
