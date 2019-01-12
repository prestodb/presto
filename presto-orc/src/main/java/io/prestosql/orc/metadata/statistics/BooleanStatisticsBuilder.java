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
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static io.prestosql.orc.metadata.statistics.BooleanStatistics.BOOLEAN_VALUE_BYTES;
import static java.util.Objects.requireNonNull;

public class BooleanStatisticsBuilder
        implements StatisticsBuilder
{
    private long nonNullValueCount;
    private long trueValueCount;

    @Override
    public void addBlock(Type type, Block block)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                addValue(type.getBoolean(block, position));
            }
        }
    }

    public void addValue(boolean value)
    {
        nonNullValueCount++;
        if (value) {
            trueValueCount++;
        }
    }

    private void addBooleanStatistics(long valueCount, BooleanStatistics value)
    {
        requireNonNull(value, "value is null");

        nonNullValueCount += valueCount;
        trueValueCount += value.getTrueValueCount();
    }

    private Optional<BooleanStatistics> buildBooleanStatistics()
    {
        if (nonNullValueCount == 0) {
            return Optional.empty();
        }
        return Optional.of(new BooleanStatistics(trueValueCount));
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        Optional<BooleanStatistics> booleanStatistics = buildBooleanStatistics();
        return new ColumnStatistics(
                nonNullValueCount,
                booleanStatistics.map(s -> BOOLEAN_VALUE_BYTES).orElse(0L),
                booleanStatistics.orElse(null),
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    public static Optional<BooleanStatistics> mergeBooleanStatistics(List<ColumnStatistics> stats)
    {
        BooleanStatisticsBuilder booleanStatisticsBuilder = new BooleanStatisticsBuilder();
        for (ColumnStatistics columnStatistics : stats) {
            BooleanStatistics partialStatistics = columnStatistics.getBooleanStatistics();
            if (columnStatistics.getNumberOfValues() > 0) {
                if (partialStatistics == null) {
                    // there are non null values but no statistics, so we can not say anything about the data
                    return Optional.empty();
                }
                booleanStatisticsBuilder.addBooleanStatistics(columnStatistics.getNumberOfValues(), partialStatistics);
            }
        }
        return booleanStatisticsBuilder.buildBooleanStatistics();
    }
}
