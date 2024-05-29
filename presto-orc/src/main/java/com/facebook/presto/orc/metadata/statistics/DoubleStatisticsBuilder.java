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
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.Type;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DoubleStatisticsBuilder
        implements StatisticsBuilder
{
    private long nonNullValueCount;
    private long storageSize;
    private long rawSize;
    private boolean hasNan;
    private double minimum = Double.POSITIVE_INFINITY;
    private double maximum = Double.NEGATIVE_INFINITY;

    @Override
    public void addBlock(Type type, Block block)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                double value;
                if (type.equals(RealType.REAL)) {
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

        nonNullValueCount += valueCount;
        minimum = Math.min(value.getMinPrimitive(), minimum);
        maximum = Math.max(value.getMaxPrimitive(), maximum);
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
        if (doubleStatistics.isPresent()) {
            return new DoubleColumnStatistics(nonNullValueCount, null, rawSize, storageSize, doubleStatistics.get());
        }
        return new ColumnStatistics(nonNullValueCount, null, rawSize, storageSize);
    }

    @Override
    public void incrementRawSize(long rawSize)
    {
        this.rawSize += rawSize;
    }

    @Override
    public void incrementSize(long storageSize)
    {
        this.storageSize += storageSize;
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
