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
package com.facebook.presto.orc.metadata.statistics;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TimestampStatisticsBuilder
        implements LongValueStatisticsBuilder
{
    private long nonNullValueCount;
    private long minimum = Long.MAX_VALUE;
    private long maximum = Long.MIN_VALUE;

    @Override
    public void addValue(long value)
    {
        nonNullValueCount++;

        minimum = Math.min(value, minimum);
        maximum = Math.max(value, maximum);
    }

    private void addTimestampStatistics(long valueCount, TimestampStatistics value)
    {
        requireNonNull(value, "value is null");
        requireNonNull(value.getMin(), "value.getMin() is null");
        requireNonNull(value.getMax(), "value.getMax() is null");

        nonNullValueCount += valueCount;
        minimum = Math.min(value.getMin(), minimum);
        maximum = Math.max(value.getMax(), maximum);
    }

    private Optional<TimestampStatistics> buildTimestampStatistics()
    {
        if (nonNullValueCount == 0) {
            return Optional.empty();
        }
        return Optional.of(new TimestampStatistics(minimum, maximum));
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        return new ColumnStatistics(
                nonNullValueCount,
                null,
                null,
                null,
                null,
                null,
                buildTimestampStatistics().orElse(null),
                null,
                null);
    }

    public static Optional<TimestampStatistics> mergeTimestampStatistics(List<ColumnStatistics> stats)
    {
        TimestampStatisticsBuilder timestampStatisticsBuilder = new TimestampStatisticsBuilder();
        for (ColumnStatistics columnStatistics : stats) {
            TimestampStatistics partialStatistics = columnStatistics.getTimestampStatistics();
            if (columnStatistics.getNumberOfValues() > 0) {
                if (partialStatistics == null) {
                    // there are non null values but no statistics, so we can not say anything about the data
                    return Optional.empty();
                }
                timestampStatisticsBuilder.addTimestampStatistics(columnStatistics.getNumberOfValues(), partialStatistics);
            }
        }
        return timestampStatisticsBuilder.buildTimestampStatistics();
    }
}
