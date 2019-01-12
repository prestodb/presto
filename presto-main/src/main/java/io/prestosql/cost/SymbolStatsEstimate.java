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
package io.prestosql.cost;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;
import static java.lang.String.format;

public class SymbolStatsEstimate
{
    private static final SymbolStatsEstimate UNKNOWN = new SymbolStatsEstimate(NEGATIVE_INFINITY, POSITIVE_INFINITY, NaN, NaN, NaN);
    private static final SymbolStatsEstimate ZERO = new SymbolStatsEstimate(NaN, NaN, 1.0, 0.0, 0.0);

    // for now we support only types which map to real domain naturally and keep low/high value as double in stats.
    private final double lowValue;
    private final double highValue;
    private final double nullsFraction;
    private final double averageRowSize;
    private final double distinctValuesCount;

    public static SymbolStatsEstimate unknown()
    {
        return UNKNOWN;
    }

    public static SymbolStatsEstimate zero()
    {
        return ZERO;
    }

    @JsonCreator
    public SymbolStatsEstimate(
            @JsonProperty("lowValue") double lowValue,
            @JsonProperty("highValue") double highValue,
            @JsonProperty("nullsFraction") double nullsFraction,
            @JsonProperty("averageRowSize") double averageRowSize,
            @JsonProperty("distinctValuesCount") double distinctValuesCount)
    {
        checkArgument(
                lowValue <= highValue || (isNaN(lowValue) && isNaN(highValue)),
                "low value must be less than or equal to high value or both values have to be NaN, got %s and %s respectively",
                lowValue,
                highValue);
        this.lowValue = lowValue;
        this.highValue = highValue;

        checkArgument(
                (0 <= nullsFraction && nullsFraction <= 1.) || isNaN(nullsFraction),
                "Nulls fraction should be within [0, 1] or NaN, got: %s",
                nullsFraction);
        boolean isEmptyRange = isNaN(lowValue) && isNaN(highValue);
        this.nullsFraction = isEmptyRange ? 1.0 : nullsFraction;

        checkArgument(averageRowSize >= 0 || isNaN(averageRowSize), "Average row size should be non-negative or NaN, got: %s", averageRowSize);
        this.averageRowSize = averageRowSize;

        checkArgument(distinctValuesCount >= 0 || isNaN(distinctValuesCount), "Distinct values count should be non-negative, got: %s", distinctValuesCount);
        // TODO normalize distinctValuesCount for an empty range (or validate it is already normalized)
        this.distinctValuesCount = distinctValuesCount;
    }

    @JsonProperty
    public double getLowValue()
    {
        return lowValue;
    }

    @JsonProperty
    public double getHighValue()
    {
        return highValue;
    }

    @JsonProperty
    public double getNullsFraction()
    {
        return nullsFraction;
    }

    public StatisticRange statisticRange()
    {
        return new StatisticRange(lowValue, highValue, distinctValuesCount);
    }

    public double getValuesFraction()
    {
        return 1.0 - nullsFraction;
    }

    @JsonProperty
    public double getAverageRowSize()
    {
        return averageRowSize;
    }

    @JsonProperty
    public double getDistinctValuesCount()
    {
        return distinctValuesCount;
    }

    public SymbolStatsEstimate mapNullsFraction(Function<Double, Double> mappingFunction)
    {
        return buildFrom(this).setNullsFraction(mappingFunction.apply(nullsFraction)).build();
    }

    public SymbolStatsEstimate mapDistinctValuesCount(Function<Double, Double> mappingFunction)
    {
        return buildFrom(this).setDistinctValuesCount(mappingFunction.apply(distinctValuesCount)).build();
    }

    public boolean isUnknown()
    {
        return this.equals(UNKNOWN);
    }

    public boolean isSingleValue()
    {
        return distinctValuesCount == 1.0
                && Double.compare(lowValue, highValue) == 0
                && !isInfinite(lowValue);
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
        SymbolStatsEstimate that = (SymbolStatsEstimate) o;
        return Double.compare(nullsFraction, that.nullsFraction) == 0 &&
                Double.compare(averageRowSize, that.averageRowSize) == 0 &&
                Double.compare(distinctValuesCount, that.distinctValuesCount) == 0 &&
                Double.compare(lowValue, that.lowValue) == 0 &&
                Double.compare(highValue, that.highValue) == 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lowValue, highValue, nullsFraction, averageRowSize, distinctValuesCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("range", format("[%s-%s]", lowValue, highValue))
                .add("nulls", nullsFraction)
                .add("ndv", distinctValuesCount)
                .add("rowSize", averageRowSize)
                .toString();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder buildFrom(SymbolStatsEstimate other)
    {
        return builder()
                .setLowValue(other.getLowValue())
                .setHighValue(other.getHighValue())
                .setNullsFraction(other.getNullsFraction())
                .setAverageRowSize(other.getAverageRowSize())
                .setDistinctValuesCount(other.getDistinctValuesCount());
    }

    public static final class Builder
    {
        private double lowValue = Double.NEGATIVE_INFINITY;
        private double highValue = Double.POSITIVE_INFINITY;
        private double nullsFraction = NaN;
        private double averageRowSize = NaN;
        private double distinctValuesCount = NaN;

        public Builder setStatisticsRange(StatisticRange range)
        {
            return setLowValue(range.getLow())
                    .setHighValue(range.getHigh())
                    .setDistinctValuesCount(range.getDistinctValuesCount());
        }

        public Builder setLowValue(double lowValue)
        {
            this.lowValue = lowValue;
            return this;
        }

        public Builder setHighValue(double highValue)
        {
            this.highValue = highValue;
            return this;
        }

        public Builder setNullsFraction(double nullsFraction)
        {
            this.nullsFraction = nullsFraction;
            return this;
        }

        public Builder setAverageRowSize(double averageRowSize)
        {
            this.averageRowSize = averageRowSize;
            return this;
        }

        public Builder setDistinctValuesCount(double distinctValuesCount)
        {
            this.distinctValuesCount = distinctValuesCount;
            return this;
        }

        public SymbolStatsEstimate build()
        {
            return new SymbolStatsEstimate(lowValue, highValue, nullsFraction, averageRowSize, distinctValuesCount);
        }
    }
}
