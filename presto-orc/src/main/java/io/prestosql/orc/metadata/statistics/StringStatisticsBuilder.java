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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.prestosql.orc.metadata.statistics.StringStatistics.STRING_VALUE_BYTES_OVERHEAD;
import static java.lang.Math.addExact;
import static java.util.Objects.requireNonNull;

public class StringStatisticsBuilder
        implements SliceColumnStatisticsBuilder
{
    private final int stringStatisticsLimitInBytes;

    private long nonNullValueCount;
    private Slice minimum;
    private Slice maximum;
    private long sum;

    public StringStatisticsBuilder(int stringStatisticsLimitInBytes)
    {
        this(stringStatisticsLimitInBytes, 0, null, null, 0);
    }

    private StringStatisticsBuilder(int stringStatisticsLimitInBytes, long nonNullValueCount, Slice minimum, Slice maximum, long sum)
    {
        this.stringStatisticsLimitInBytes = stringStatisticsLimitInBytes;
        this.nonNullValueCount = nonNullValueCount;
        this.minimum = minimum;
        this.maximum = maximum;
        this.sum = sum;
    }

    public StringStatisticsBuilder withStringStatisticsLimit(int limitInBytes)
    {
        checkArgument(limitInBytes >= 0, "limitInBytes is less than 0");
        return new StringStatisticsBuilder(limitInBytes, nonNullValueCount, minimum, maximum, sum);
    }

    public long getNonNullValueCount()
    {
        return nonNullValueCount;
    }

    @Override
    public void addValue(Slice value)
    {
        requireNonNull(value, "value is null");

        if (nonNullValueCount == 0) {
            checkState(minimum == null && maximum == null);
            minimum = value;
            maximum = value;
        }
        else if (minimum != null && value.compareTo(minimum) <= 0) {
            minimum = value;
        }
        else if (maximum != null && value.compareTo(maximum) >= 0) {
            maximum = value;
        }

        nonNullValueCount++;
        sum = addExact(sum, value.length());
    }

    /**
     * This method can only be used in merging stats.
     * It assumes min or max could be nulls.
     */
    private void addStringStatistics(long valueCount, StringStatistics value)
    {
        requireNonNull(value, "value is null");
        checkArgument(valueCount > 0, "valueCount is 0");
        checkArgument(value.getMin() != null || value.getMax() != null, "min and max cannot both be null");

        if (nonNullValueCount == 0) {
            checkState(minimum == null && maximum == null);
            minimum = value.getMin();
            maximum = value.getMax();
        }
        else {
            if (minimum != null && (value.getMin() == null || minimum.compareTo(value.getMin()) > 0)) {
                minimum = value.getMin();
            }
            if (maximum != null && (value.getMax() == null || maximum.compareTo(value.getMax()) < 0)) {
                maximum = value.getMax();
            }
        }

        nonNullValueCount += valueCount;
        sum = addExact(sum, value.getSum());
    }

    private Optional<StringStatistics> buildStringStatistics()
    {
        if (nonNullValueCount == 0) {
            return Optional.empty();
        }
        minimum = dropStringMinMaxIfNecessary(minimum);
        maximum = dropStringMinMaxIfNecessary(maximum);
        if (minimum == null && maximum == null) {
            // Create string stats only when min or max is not null.
            // This corresponds to the behavior of metadata reader.
            return Optional.empty();
        }
        return Optional.of(new StringStatistics(minimum, maximum, sum));
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        Optional<StringStatistics> stringStatistics = buildStringStatistics();
        stringStatistics.ifPresent(s -> verify(nonNullValueCount > 0));
        return new ColumnStatistics(
                nonNullValueCount,
                stringStatistics.map(s -> STRING_VALUE_BYTES_OVERHEAD + sum / nonNullValueCount).orElse(0L),
                null,
                null,
                null,
                stringStatistics.orElse(null),
                null,
                null,
                null,
                null);
    }

    public static Optional<StringStatistics> mergeStringStatistics(List<ColumnStatistics> stats)
    {
        // no need to set the stats limit for the builder given we assume the given stats are within the same limit
        StringStatisticsBuilder stringStatisticsBuilder = new StringStatisticsBuilder(Integer.MAX_VALUE);
        for (ColumnStatistics columnStatistics : stats) {
            StringStatistics partialStatistics = columnStatistics.getStringStatistics();
            if (columnStatistics.getNumberOfValues() > 0) {
                if (partialStatistics == null || (partialStatistics.getMin() == null && partialStatistics.getMax() == null)) {
                    // there are non null values but no statistics, so we can not say anything about the data
                    return Optional.empty();
                }
                stringStatisticsBuilder.addStringStatistics(columnStatistics.getNumberOfValues(), partialStatistics);
            }
        }
        return stringStatisticsBuilder.buildStringStatistics();
    }

    private Slice dropStringMinMaxIfNecessary(Slice minOrMax)
    {
        if (minOrMax == null || minOrMax.length() > stringStatisticsLimitInBytes) {
            return null;
        }

        // Do not hold the entire slice where the actual stats could be small
        if (minOrMax.isCompact()) {
            return minOrMax;
        }
        return Slices.copyOf(minOrMax);
    }
}
