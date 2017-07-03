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
package com.facebook.presto.cost;

import java.util.Objects;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;

public class SymbolStatsEstimate
{
    public static final SymbolStatsEstimate UNKNOWN_STATS = SymbolStatsEstimate.builder().build();

    // for now we support only types which map to real domain naturally and keep low/high value as double in stats.
    private final double lowValue;
    private final double highValue;
    private final double nullsFraction;
    private final double averageRowSize;
    private final double distinctValuesCount;

    public SymbolStatsEstimate(double lowValue, double highValue, double nullsFraction, double averageRowSize, double distinctValuesCount)
    {
        checkArgument(lowValue <= highValue || (isNaN(lowValue) && isNaN(highValue)), "lowValue must be less than or equal to highValue or both values have to be NaN");
        this.lowValue = lowValue;
        this.highValue = highValue;
        this.nullsFraction = nullsFraction;
        this.averageRowSize = averageRowSize;
        this.distinctValuesCount = distinctValuesCount;
    }

    public double getLowValue()
    {
        return lowValue;
    }

    public double getHighValue()
    {
        return highValue;
    }

    public boolean hasEmptyRange()
    {
        return isNaN(lowValue) && isNaN(highValue);
    }

    public double getNullsFraction()
    {
        if (hasEmptyRange()) {
            return 1.0;
        }
        return nullsFraction;
    }

    public StatisticRange statisticRange()
    {
        return  new StatisticRange(lowValue, highValue, distinctValuesCount);
    }

    public double getValuesFraction()
    {
        return 1.0 - nullsFraction;
    }

    public double getAverageRowSize()
    {
        return averageRowSize;
    }

    public double getDistinctValuesCount()
    {
        return distinctValuesCount;
    }

    public SymbolStatsEstimate mapLowValue(Function<Double, Double> mappingFunction)
    {
        return buildFrom(this).setLowValue(mappingFunction.apply(lowValue)).build();
    }

    public SymbolStatsEstimate mapHighValue(Function<Double, Double> mappingFunction)
    {
        return buildFrom(this).setHighValue(mappingFunction.apply(highValue)).build();
    }

    public SymbolStatsEstimate mapNullsFraction(Function<Double, Double> mappingFunction)
    {
        return buildFrom(this).setNullsFraction(mappingFunction.apply(nullsFraction)).build();
    }

    public SymbolStatsEstimate mapDistinctValuesCount(Function<Double, Double> mappingFunction)
    {
        return buildFrom(this).setDistinctValuesCount(mappingFunction.apply(distinctValuesCount)).build();
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
        return Double.compare(that.nullsFraction, nullsFraction) == 0 &&
                Double.compare(that.averageRowSize, averageRowSize) == 0 &&
                Double.compare(that.distinctValuesCount, distinctValuesCount) == 0 &&
                Objects.equals(lowValue, that.lowValue) &&
                Objects.equals(highValue, that.highValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lowValue, highValue, nullsFraction, averageRowSize, distinctValuesCount);
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
