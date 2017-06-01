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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.statistics.Estimate.unknownValue;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class RangeColumnStatistics
{
    public static final String DATA_SIZE_STATISTIC_KEY = "data_size";
    public static final String FRACTION_STATISTICS_KEY = "fraction";
    public static final String DISTINCT_VALUES_STATISTICS_KEY = "distinct_values_count";

    private final Optional<Object> lowValue;
    private final Optional<Object> highValue;
    private final Map<String, Estimate> statistics;

    public RangeColumnStatistics(
            Optional<Object> lowValue,
            Optional<Object> highValue,
            Estimate fraction,
            Estimate dataSize,
            Estimate distinctValuesCount)
    {
        this.lowValue = requireNonNull(lowValue, "lowValue can not be null");
        this.highValue = requireNonNull(highValue, "highValue can not be null");
        requireNonNull(fraction, "fraction can not be null");
        requireNonNull(dataSize, "dataSize can not be null");
        requireNonNull(distinctValuesCount, "distinctValuesCount can not be null");
        this.statistics = createStatisticsMap(dataSize, fraction, distinctValuesCount);
    }

    private static Map<String, Estimate> createStatisticsMap(
            Estimate dataSize,
            Estimate fraction,
            Estimate distinctValuesCount)
    {
        Map<String, Estimate> statistics = new HashMap<>();
        statistics.put(FRACTION_STATISTICS_KEY, fraction);
        statistics.put(DATA_SIZE_STATISTIC_KEY, dataSize);
        statistics.put(DISTINCT_VALUES_STATISTICS_KEY, distinctValuesCount);
        return unmodifiableMap(statistics);
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
        return statistics.get(DATA_SIZE_STATISTIC_KEY);
    }

    public Estimate getFraction()
    {
        return statistics.get(FRACTION_STATISTICS_KEY);
    }

    public Estimate getDistinctValuesCount()
    {
        return statistics.get(DISTINCT_VALUES_STATISTICS_KEY);
    }

    public Map<String, Estimate> getStatistics()
    {
        return statistics;
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
            this.dataSize = requireNonNull(dataSize, "dataSize can not be null");
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
