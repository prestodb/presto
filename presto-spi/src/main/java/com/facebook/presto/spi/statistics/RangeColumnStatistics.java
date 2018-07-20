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

import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.statistics.Estimate.unknownValue;
import static java.util.Objects.requireNonNull;

public final class RangeColumnStatistics
{
    private final Optional<Object> lowValue;
    private final Optional<Object> highValue;
    private final Estimate fraction;
    private final Estimate dataSize;
    private final Estimate distinctValuesCount;

    private RangeColumnStatistics(
            Optional<Object> lowValue,
            Optional<Object> highValue,
            Estimate fraction,
            Estimate dataSize,
            Estimate distinctValuesCount)
    {
        this.lowValue = requireNonNull(lowValue, "lowValue can not be null");
        this.highValue = requireNonNull(highValue, "highValue can not be null");
        this.fraction = requireNonNull(fraction, "fraction can not be null");
        this.dataSize = requireNonNull(dataSize, "dataSize can not be null");
        this.distinctValuesCount = requireNonNull(distinctValuesCount, "distinctValuesCount can not be null");
    }

    public Optional<Object> getLowValue()
    {
        return lowValue;
    }

    public Optional<Object> getHighValue()
    {
        return highValue;
    }

    public Estimate getDataSize()
    {
        return dataSize;
    }

    public Estimate getFraction()
    {
        return fraction;
    }

    public Estimate getDistinctValuesCount()
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
        RangeColumnStatistics that = (RangeColumnStatistics) o;
        return Objects.equals(lowValue, that.lowValue) &&
                Objects.equals(highValue, that.highValue) &&
                Objects.equals(fraction, that.fraction) &&
                Objects.equals(dataSize, that.dataSize) &&
                Objects.equals(distinctValuesCount, that.distinctValuesCount);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lowValue, highValue, fraction, dataSize, distinctValuesCount);
    }

    @Override
    public String toString()
    {
        return "RangeColumnStatistics{" + "lowValue=" + lowValue +
                ", highValue=" + highValue +
                ", fraction=" + fraction +
                ", dataSize=" + dataSize +
                ", distinctValuesCount=" + distinctValuesCount +
                '}';
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Optional<Object> lowValue = Optional.empty();
        private Optional<Object> highValue = Optional.empty();
        private Estimate dataSize = unknownValue();
        private Estimate fraction = unknownValue();
        private Estimate distinctValuesCount = unknownValue();

        public Builder setLowValue(Optional<Object> lowValue)
        {
            this.lowValue = lowValue;
            return this;
        }

        public Builder setHighValue(Optional<Object> highValue)
        {
            this.highValue = highValue;
            return this;
        }

        public Builder setFraction(Estimate fraction)
        {
            this.fraction = fraction;
            return this;
        }

        public Builder setDataSize(Estimate dataSize)
        {
            this.dataSize = dataSize;
            return this;
        }

        public Builder setDistinctValuesCount(Estimate distinctValuesCount)
        {
            this.distinctValuesCount = distinctValuesCount;
            return this;
        }

        public RangeColumnStatistics build()
        {
            return new RangeColumnStatistics(lowValue, highValue, fraction, dataSize, distinctValuesCount);
        }
    }
}
