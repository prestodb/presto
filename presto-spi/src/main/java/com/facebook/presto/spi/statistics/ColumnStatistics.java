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

import static com.facebook.presto.spi.statistics.Estimate.unknownValue;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public final class ColumnStatistics
{
    private final Map<String, Estimate> statistics;
    private static final String DATA_SIZE_STATISTIC_KEY = "data_size";
    private static final String NULLS_COUNT_STATISTIC_KEY = "nulls_count";
    private static final String DISTINCT_VALUES_STATITIC_KEY = "distinct_values_count";

    private ColumnStatistics(Estimate dataSize, Estimate nullsCount, Estimate distinctValuesCount)
    {
        requireNonNull(dataSize, "dataSize can not be null");
        statistics = createStatisticsMap(dataSize, nullsCount, distinctValuesCount);
    }

    private static Map<String, Estimate> createStatisticsMap(Estimate dataSize, Estimate nullsCount, Estimate distinctValuesCount)
    {
        Map<String, Estimate> statistics = new HashMap<>();
        statistics.put(DATA_SIZE_STATISTIC_KEY, dataSize);
        statistics.put(NULLS_COUNT_STATISTIC_KEY, nullsCount);
        statistics.put(DISTINCT_VALUES_STATITIC_KEY, distinctValuesCount);
        return unmodifiableMap(statistics);
    }

    public Estimate getDataSize()
    {
        return statistics.get(DATA_SIZE_STATISTIC_KEY);
    }

    public Estimate getNullsCount()
    {
        return statistics.get(NULLS_COUNT_STATISTIC_KEY);
    }

    public Estimate getDistinctValuesCount()
    {
        return statistics.get(DISTINCT_VALUES_STATITIC_KEY);
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
        private Estimate dataSize = unknownValue();
        private Estimate nullsCount = unknownValue();
        private Estimate distinctValuesCount = unknownValue();

        public Builder setDataSize(Estimate dataSize)
        {
            this.dataSize = requireNonNull(dataSize, "dataSize can not be null");
            return this;
        }

        public Builder setNullsCount(Estimate nullsCount)
        {
            this.nullsCount = nullsCount;
            return this;
        }

        public Builder setDistinctValuesCount(Estimate distinctValuesCount)
        {
            this.distinctValuesCount = distinctValuesCount;
            return this;
        }

        public ColumnStatistics build()
        {
            return new ColumnStatistics(dataSize, nullsCount, distinctValuesCount);
        }
    }
}
