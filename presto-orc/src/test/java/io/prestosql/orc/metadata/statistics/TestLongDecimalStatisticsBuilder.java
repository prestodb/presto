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

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.orc.metadata.statistics.AbstractStatisticsBuilderTest.StatisticsType.DECIMAL;
import static io.prestosql.orc.metadata.statistics.DecimalStatistics.DECIMAL_VALUE_BYTES_OVERHEAD;
import static io.prestosql.orc.metadata.statistics.LongDecimalStatisticsBuilder.LONG_DECIMAL_VALUE_BYTES;
import static java.math.BigDecimal.ZERO;

public class TestLongDecimalStatisticsBuilder
        extends AbstractStatisticsBuilderTest<LongDecimalStatisticsBuilder, BigDecimal>
{
    private static final BigDecimal MEDIUM_VALUE = new BigDecimal("890.37492");
    private static final BigDecimal LARGE_POSITIVE_VALUE = new BigDecimal("123456789012345678901234567890.12345");
    private static final BigDecimal LARGE_NEGATIVE_VALUE = LARGE_POSITIVE_VALUE.negate();
    private static final List<Long> ZERO_TO_42 = ContiguousSet.create(Range.closed(0L, 42L), DiscreteDomain.longs()).asList();

    public TestLongDecimalStatisticsBuilder()
    {
        super(DECIMAL, LongDecimalStatisticsBuilder::new, LongDecimalStatisticsBuilder::addValue);
    }

    @Test
    public void testMinMaxValues()
    {
        assertMinMaxValues(ZERO, ZERO);
        assertMinMaxValues(MEDIUM_VALUE, MEDIUM_VALUE);
        assertMinMaxValues(LARGE_NEGATIVE_VALUE, LARGE_NEGATIVE_VALUE);
        assertMinMaxValues(LARGE_POSITIVE_VALUE, LARGE_POSITIVE_VALUE);

        assertMinMaxValues(ZERO, MEDIUM_VALUE);
        assertMinMaxValues(MEDIUM_VALUE, MEDIUM_VALUE);
        assertMinMaxValues(LARGE_NEGATIVE_VALUE, MEDIUM_VALUE);
        assertMinMaxValues(MEDIUM_VALUE, LARGE_POSITIVE_VALUE);
        assertMinMaxValues(LARGE_NEGATIVE_VALUE, LARGE_POSITIVE_VALUE);

        assertValues(ZERO, MEDIUM_VALUE, toBigDecimalList(ZERO, MEDIUM_VALUE, ZERO_TO_42));
        assertValues(LARGE_NEGATIVE_VALUE, MEDIUM_VALUE, toBigDecimalList(LARGE_NEGATIVE_VALUE, MEDIUM_VALUE, ZERO_TO_42));
        assertValues(MEDIUM_VALUE, LARGE_POSITIVE_VALUE, toBigDecimalList(MEDIUM_VALUE, LARGE_POSITIVE_VALUE, ZERO_TO_42));
        assertValues(LARGE_NEGATIVE_VALUE, LARGE_POSITIVE_VALUE, toBigDecimalList(LARGE_NEGATIVE_VALUE, LARGE_POSITIVE_VALUE, ZERO_TO_42));
    }

    @Test
    public void testMinAverageValueBytes()
    {
        long longDecimalBytes = DECIMAL_VALUE_BYTES_OVERHEAD + LONG_DECIMAL_VALUE_BYTES;
        assertMinAverageValueBytes(0L, ImmutableList.of());
        assertMinAverageValueBytes(longDecimalBytes, ImmutableList.of(LARGE_POSITIVE_VALUE));
        assertMinAverageValueBytes(longDecimalBytes, ImmutableList.of(LARGE_NEGATIVE_VALUE));
        assertMinAverageValueBytes(longDecimalBytes, ImmutableList.of(LARGE_POSITIVE_VALUE, LARGE_POSITIVE_VALUE, LARGE_POSITIVE_VALUE, LARGE_NEGATIVE_VALUE));
    }

    private static List<BigDecimal> toBigDecimalList(BigDecimal minValue, BigDecimal maxValue, List<Long> values)
    {
        return values.stream()
                .flatMap(value -> Stream.of(maxValue.subtract(BigDecimal.valueOf(value)), minValue.add(BigDecimal.valueOf(value))))
                .collect(toImmutableList());
    }
}
