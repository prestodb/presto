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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import static io.prestosql.orc.metadata.statistics.DecimalStatistics.DECIMAL_VALUE_BYTES_OVERHEAD;

public class ShortDecimalStatisticsBuilder
        implements LongValueStatisticsBuilder
{
    public static final long SHORT_DECIMAL_VALUE_BYTES = 8L;

    private final int scale;

    private long nonNullValueCount;
    private long minimum = Long.MAX_VALUE;
    private long maximum = Long.MIN_VALUE;

    public ShortDecimalStatisticsBuilder(int scale)
    {
        this.scale = scale;
    }

    @Override
    public void addValue(long value)
    {
        nonNullValueCount++;

        minimum = Math.min(value, minimum);
        maximum = Math.max(value, maximum);
    }

    private Optional<DecimalStatistics> buildDecimalStatistics()
    {
        if (nonNullValueCount == 0) {
            return Optional.empty();
        }
        return Optional.of(new DecimalStatistics(
                new BigDecimal(BigInteger.valueOf(minimum), scale),
                new BigDecimal(BigInteger.valueOf(maximum), scale),
                SHORT_DECIMAL_VALUE_BYTES));
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        Optional<DecimalStatistics> decimalStatistics = buildDecimalStatistics();
        return new ColumnStatistics(
                nonNullValueCount,
                decimalStatistics.map(s -> DECIMAL_VALUE_BYTES_OVERHEAD + SHORT_DECIMAL_VALUE_BYTES).orElse(0L),
                null,
                null,
                null,
                null,
                null,
                decimalStatistics.orElse(null),
                null,
                null);
    }
}
