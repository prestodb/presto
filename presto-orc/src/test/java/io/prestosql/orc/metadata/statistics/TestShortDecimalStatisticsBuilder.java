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
import java.math.BigInteger;

import static io.prestosql.orc.metadata.statistics.AbstractStatisticsBuilderTest.StatisticsType.DECIMAL;
import static io.prestosql.orc.metadata.statistics.DecimalStatistics.DECIMAL_VALUE_BYTES_OVERHEAD;
import static io.prestosql.orc.metadata.statistics.ShortDecimalStatisticsBuilder.SHORT_DECIMAL_VALUE_BYTES;
import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestShortDecimalStatisticsBuilder
        extends AbstractStatisticsBuilderTest<ShortDecimalStatisticsBuilder, Long>
{
    private static final int SCALE = 8;

    public TestShortDecimalStatisticsBuilder()
    {
        super(DECIMAL, () -> new ShortDecimalStatisticsBuilder(SCALE), ShortDecimalStatisticsBuilder::addValue);
    }

    @Test
    public void testMinMaxValues()
    {
        assertMinMaxValues(0L, 0L);
        assertMinMaxValues(42L, 42L);
        assertMinMaxValues(MIN_VALUE, MIN_VALUE);
        assertMinMaxValues(MAX_VALUE, MAX_VALUE);

        assertMinMaxValues(0L, 42L);
        assertMinMaxValues(42L, 42L);
        assertMinMaxValues(MIN_VALUE, 42L);
        assertMinMaxValues(42L, MAX_VALUE);
        assertMinMaxValues(MIN_VALUE, MAX_VALUE);

        assertValues(-42L, 0L, ContiguousSet.create(Range.closed(-42L, 0L), DiscreteDomain.longs()).asList());
        assertValues(-42L, 42L, ContiguousSet.create(Range.closed(-42L, 42L), DiscreteDomain.longs()).asList());
        assertValues(0L, 42L, ContiguousSet.create(Range.closed(0L, 42L), DiscreteDomain.longs()).asList());
        assertValues(MIN_VALUE, MIN_VALUE + 42, ContiguousSet.create(Range.closed(MIN_VALUE, MIN_VALUE + 42), DiscreteDomain.longs()).asList());
        assertValues(MAX_VALUE - 42L, MAX_VALUE, ContiguousSet.create(Range.closed(MAX_VALUE - 42L, MAX_VALUE), DiscreteDomain.longs()).asList());
    }

    @Test
    public void testMinAverageValueBytes()
    {
        long shortDecimalBytes = DECIMAL_VALUE_BYTES_OVERHEAD + SHORT_DECIMAL_VALUE_BYTES;
        assertMinAverageValueBytes(0L, ImmutableList.of());
        assertMinAverageValueBytes(shortDecimalBytes, ImmutableList.of(0L));
        assertMinAverageValueBytes(shortDecimalBytes, ImmutableList.of(42L));
        assertMinAverageValueBytes(shortDecimalBytes, ImmutableList.of(0L, 1L, 42L, 44L, 52L));
    }

    @Override
    void assertRangeStatistics(RangeStatistics<?> rangeStatistics, Long expectedMin, Long expectedMax)
    {
        assertNotNull(rangeStatistics);
        assertEquals(rangeStatistics.getMin(), new BigDecimal(BigInteger.valueOf(expectedMin), SCALE));
        assertEquals(rangeStatistics.getMax(), new BigDecimal(BigInteger.valueOf(expectedMax), SCALE));
    }
}
