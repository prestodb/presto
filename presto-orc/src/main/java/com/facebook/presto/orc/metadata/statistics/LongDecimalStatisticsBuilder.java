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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.Type;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class LongDecimalStatisticsBuilder
        implements StatisticsBuilder
{
    public static final long LONG_DECIMAL_VALUE_BYTES = 16L;

    private long nonNullValueCount;
    private BigDecimal minimum;
    private BigDecimal maximum;

    @Override
    public void addBlock(Type type, Block block)
    {
        int scale = ((DecimalType) type).getScale();
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                Slice value = type.getSlice(block, position);
                addValue(new BigDecimal(Decimals.decodeUnscaledValue(value), scale));
            }
        }
    }

    public void addValue(BigDecimal value)
    {
        requireNonNull(value, "value is null");

        nonNullValueCount++;

        if (minimum == null) {
            minimum = value;
            maximum = value;
        }
        else {
            minimum = minimum.min(value);
            maximum = maximum.max(value);
        }
    }

    private void addDecimalStatistics(long valueCount, DecimalStatistics value)
    {
        requireNonNull(value, "value is null");
        requireNonNull(value.getMin(), "value.getMin() is null");
        requireNonNull(value.getMax(), "value.getMax() is null");

        nonNullValueCount += valueCount;
        if (minimum == null) {
            minimum = value.getMin();
            maximum = value.getMax();
        }
        else {
            minimum = minimum.min(value.getMin());
            maximum = maximum.max(value.getMax());
        }
    }

    private Optional<DecimalStatistics> buildDecimalStatistics(long decimalSizeInBytes)
    {
        if (nonNullValueCount == 0) {
            return Optional.empty();
        }
        checkState(minimum != null && maximum != null);
        return Optional.of(new DecimalStatistics(minimum, maximum, decimalSizeInBytes));
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        Optional<DecimalStatistics> decimalStatistics = buildDecimalStatistics(LONG_DECIMAL_VALUE_BYTES);
        if (decimalStatistics.isPresent()) {
            return new DecimalColumnStatistics(nonNullValueCount, null, decimalStatistics.get());
        }
        return new ColumnStatistics(nonNullValueCount, null);
    }

    public static Optional<DecimalStatistics> mergeDecimalStatistics(List<ColumnStatistics> stats)
    {
        LongDecimalStatisticsBuilder decimalStatisticsBuilder = new LongDecimalStatisticsBuilder();
        long decimalSizeInBytes = 0;
        for (ColumnStatistics columnStatistics : stats) {
            DecimalStatistics partialStatistics = columnStatistics.getDecimalStatistics();
            if (columnStatistics.getNumberOfValues() > 0) {
                if (partialStatistics == null) {
                    // there are non null values but no statistics, so we can not say anything about the data
                    return Optional.empty();
                }
                decimalStatisticsBuilder.addDecimalStatistics(columnStatistics.getNumberOfValues(), partialStatistics);
                decimalSizeInBytes = max(decimalSizeInBytes, partialStatistics.getDecimalSizeInBytes());
            }
        }
        return decimalStatisticsBuilder.buildDecimalStatistics(decimalSizeInBytes);
    }
}
