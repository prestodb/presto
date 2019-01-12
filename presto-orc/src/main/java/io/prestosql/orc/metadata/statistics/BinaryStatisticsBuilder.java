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

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.prestosql.orc.metadata.statistics.BinaryStatistics.BINARY_VALUE_BYTES_OVERHEAD;
import static java.util.Objects.requireNonNull;

public class BinaryStatisticsBuilder
        implements SliceColumnStatisticsBuilder
{
    private long nonNullValueCount;
    private long sum;

    @Override
    public void addValue(Slice value)
    {
        requireNonNull(value, "value is null");

        sum += value.length();
        nonNullValueCount++;
    }

    private Optional<BinaryStatistics> buildBinaryStatistics()
    {
        if (nonNullValueCount == 0) {
            return Optional.empty();
        }
        return Optional.of(new BinaryStatistics(sum));
    }

    private void addBinaryStatistics(long valueCount, BinaryStatistics value)
    {
        requireNonNull(value, "value is null");

        nonNullValueCount += valueCount;
        sum += value.getSum();
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        Optional<BinaryStatistics> binaryStatistics = buildBinaryStatistics();
        binaryStatistics.ifPresent(s -> verify(nonNullValueCount > 0));
        return new ColumnStatistics(
                nonNullValueCount,
                binaryStatistics.map(s -> BINARY_VALUE_BYTES_OVERHEAD + sum / nonNullValueCount).orElse(0L),
                null,
                null,
                null,
                null,
                null,
                null,
                binaryStatistics.orElse(null),
                null);
    }

    public static Optional<BinaryStatistics> mergeBinaryStatistics(List<ColumnStatistics> stats)
    {
        BinaryStatisticsBuilder binaryStatisticsBuilder = new BinaryStatisticsBuilder();
        for (ColumnStatistics columnStatistics : stats) {
            BinaryStatistics partialStatistics = columnStatistics.getBinaryStatistics();
            if (columnStatistics.getNumberOfValues() > 0) {
                if (partialStatistics == null) {
                    // there are non null values but no statistics, so we can not say anything about the data
                    return Optional.empty();
                }
                binaryStatisticsBuilder.addBinaryStatistics(columnStatistics.getNumberOfValues(), partialStatistics);
            }
        }
        return binaryStatisticsBuilder.buildBinaryStatistics();
    }
}
