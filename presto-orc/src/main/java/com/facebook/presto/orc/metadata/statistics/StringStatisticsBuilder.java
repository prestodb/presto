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

import io.airlift.slice.Slice;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class StringStatisticsBuilder
        implements SliceColumnStatisticsBuilder
{
    private long nonNullValueCount;
    private Slice minimum;
    private Slice maximum;

    public long getNonNullValueCount()
    {
        return nonNullValueCount;
    }

    @Override
    public void addValue(Slice value)
    {
        requireNonNull(value, "value is null");

        nonNullValueCount++;

        if (minimum == null) {
            minimum = value;
            maximum = value;
        }
        else if (value.compareTo(minimum) <= 0) {
            minimum = value;
        }
        else if (value.compareTo(maximum) >= 0) {
            maximum = value;
        }
    }

    private void addStringStatistics(long valueCount, StringStatistics value)
    {
        requireNonNull(value, "value is null");
        requireNonNull(value.getMin(), "value.getMin() is null");
        requireNonNull(value.getMax(), "value.getMax() is null");

        nonNullValueCount += valueCount;
        addValue(value.getMin());
        addValue(value.getMax());
    }

    private Optional<StringStatistics> buildStringStatistics()
    {
        if (nonNullValueCount == 0) {
            return Optional.empty();
        }
        return Optional.of(new StringStatistics(minimum, maximum));
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        return new ColumnStatistics(
                nonNullValueCount,
                null,
                null,
                null,
                buildStringStatistics().orElse(null),
                null,
                null,
                null);
    }

    public static Optional<StringStatistics> mergeStringStatistics(List<ColumnStatistics> stats)
    {
        StringStatisticsBuilder stringStatisticsBuilder = new StringStatisticsBuilder();
        for (ColumnStatistics columnStatistics : stats) {
            StringStatistics partialStatistics = columnStatistics.getStringStatistics();
            if (columnStatistics.getNumberOfValues() > 0) {
                if (partialStatistics == null) {
                    // there are non null values but no statistics, so we can not say anything about the data
                    return Optional.empty();
                }
                stringStatisticsBuilder.addStringStatistics(columnStatistics.getNumberOfValues(), partialStatistics);
            }
        }
        return stringStatisticsBuilder.buildStringStatistics();
    }
}
