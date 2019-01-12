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
package io.prestosql.orc.metadata.statistics;

import java.util.List;
import java.util.Optional;

import static io.prestosql.orc.metadata.statistics.DateStatistics.DATE_VALUE_BYTES;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class DateStatisticsBuilder
        implements LongValueStatisticsBuilder
{
    private long nonNullValueCount;
    private int minimum = Integer.MAX_VALUE;
    private int maximum = Integer.MIN_VALUE;

    @Override
    public void addValue(long value)
    {
        nonNullValueCount++;

        int intValue = toIntExact(value);
        minimum = Math.min(intValue, minimum);
        maximum = Math.max(intValue, maximum);
    }

    private void addDateStatistics(long valueCount, DateStatistics value)
    {
        requireNonNull(value, "value is null");
        requireNonNull(value.getMin(), "value.getMin() is null");
        requireNonNull(value.getMax(), "value.getMax() is null");

        nonNullValueCount += valueCount;
        minimum = Math.min(value.getMin(), minimum);
        maximum = Math.max(value.getMax(), maximum);
    }

    private Optional<DateStatistics> buildDateStatistics()
    {
        if (nonNullValueCount == 0) {
            return Optional.empty();
        }
        return Optional.of(new DateStatistics(minimum, maximum));
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        Optional<DateStatistics> dateStatistics = buildDateStatistics();
        return new ColumnStatistics(
                nonNullValueCount,
                dateStatistics.map(s -> DATE_VALUE_BYTES).orElse(0L),
                null,
                null,
                null,
                null,
                dateStatistics.orElse(null),
                null,
                null,
                null);
    }

    public static Optional<DateStatistics> mergeDateStatistics(List<ColumnStatistics> stats)
    {
        DateStatisticsBuilder dateStatisticsBuilder = new DateStatisticsBuilder();
        for (ColumnStatistics columnStatistics : stats) {
            DateStatistics partialStatistics = columnStatistics.getDateStatistics();
            if (columnStatistics.getNumberOfValues() > 0) {
                if (partialStatistics == null) {
                    // there are non null values but no statistics, so we can not say anything about the data
                    return Optional.empty();
                }
                dateStatisticsBuilder.addDateStatistics(columnStatistics.getNumberOfValues(), partialStatistics);
            }
        }
        return dateStatisticsBuilder.buildDateStatistics();
    }
}
