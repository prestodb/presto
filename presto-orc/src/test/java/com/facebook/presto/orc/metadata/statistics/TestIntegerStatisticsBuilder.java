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

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import org.testng.annotations.Test;

import static com.facebook.presto.orc.metadata.statistics.AbstractStatisticsBuilderTest.StatisticsType.INTEGER;
import static com.facebook.presto.orc.metadata.statistics.IntegerStatistics.INTEGER_VALUE_BYTES;
import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;

public class TestIntegerStatisticsBuilder
        extends AbstractStatisticsBuilderTest<IntegerStatisticsBuilder, Long>
{
    public TestIntegerStatisticsBuilder()
    {
        super(INTEGER, IntegerStatisticsBuilder::new, IntegerStatisticsBuilder::addValue);
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
        assertMinAverageValueBytes(0L, ImmutableList.of());
        assertMinAverageValueBytes(INTEGER_VALUE_BYTES, ImmutableList.of(42L));
        assertMinAverageValueBytes(INTEGER_VALUE_BYTES, ImmutableList.of(0L));
        assertMinAverageValueBytes(INTEGER_VALUE_BYTES, ImmutableList.of(0L, 42L, 42L, 43L));
    }
}
