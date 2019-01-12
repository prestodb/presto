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

import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static io.prestosql.orc.metadata.statistics.DoubleStatistics.DOUBLE_VALUE_BYTES;
import static java.util.Objects.requireNonNull;

public class DoubleStatisticsBuilder
        implements StatisticsBuilder
{
    private long nonNullValueCount;
    private boolean hasNan;
    private double minimum = Double.POSITIVE_INFINITY;
    private double maximum = Double.NEGATIVE_INFINITY;

    @Override
    public void addBlock(Type type, Block block)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                double value;
                if (type == RealType.REAL) {
                    value = Float.intBitsToFloat((int) type.getLong(block, position));
                }
                else {
                    value = type.getDouble(block, position);
                }
                addValue(value);
            }
        }
    }

    public void addValue(double value)
    {
        nonNullValueCount++;
        if (Double.isNaN(value)) {
            hasNan = true;
        }
        else {
            minimum = Math.min(value, minimum);
            maximum = Math.max(value, maximum);
        }
    }

    private void addDoubleStatistics(long valueCount, DoubleStatistics value)
    {
        requireNonNull(value, "value is null");
        requireNonNull(value.getMin(), "value.getMin() is null");
        requireNonNull(value.getMax(), "value.getMax() is null");

        nonNullValueCount += valueCount;
        minimum = Math.min(value.getMin(), minimum);
        maximum = Math.max(value.getMax(), maximum);
    }

    private Optional<DoubleStatistics> buildDoubleStatistics()
    {
        // if there are NaN values we can not say anything about the data
        if (nonNullValueCount == 0 || hasNan) {
            return Optional.empty();
        }
        return Optional.of(new DoubleStatistics(minimum, maximum));
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        Optional<DoubleStatistics> doubleStatistics = buildDoubleStatistics();
        return new ColumnStatistics(
                nonNullValueCount,
                doubleStatistics.map(s -> DOUBLE_VALUE_BYTES).orElse(0L),
                null,
                null,
                doubleStatistics.orElse(null),
                null,
                null,
                null,
                null,
                null);
    }

    public static Optional<DoubleStatistics> mergeDoubleStatistics(List<ColumnStatistics> stats)
    {
        DoubleStatisticsBuilder doubleStatisticsBuilder = new DoubleStatisticsBuilder();
        for (ColumnStatistics columnStatistics : stats) {
            DoubleStatistics partialStatistics = columnStatistics.getDoubleStatistics();
            if (columnStatistics.getNumberOfValues() > 0) {
                if (partialStatistics == null) {
                    // there are non null values but no statistics, so we can not say anything about the data
                    return Optional.empty();
                }
                doubleStatisticsBuilder.addDoubleStatistics(columnStatistics.getNumberOfValues(), partialStatistics);
            }
        }
        return doubleStatisticsBuilder.buildDoubleStatistics();
    }
}
