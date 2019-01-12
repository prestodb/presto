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

import static io.prestosql.orc.metadata.statistics.AbstractStatisticsBuilderTest.StatisticsType.DATE;
import static io.prestosql.orc.metadata.statistics.DateStatistics.DATE_VALUE_BYTES;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.MIN_VALUE;
import static org.testng.Assert.fail;

public class TestDateStatisticsBuilder
        extends AbstractStatisticsBuilderTest<DateStatisticsBuilder, Integer>
{
    public TestDateStatisticsBuilder()
    {
        super(DATE, DateStatisticsBuilder::new, DateStatisticsBuilder::addValue);
    }

    @Test
    public void testMinMaxValues()
    {
        assertMinMaxValues(0, 0);
        assertMinMaxValues(42, 42);
        assertMinMaxValues(MIN_VALUE, MIN_VALUE);
        assertMinMaxValues(MAX_VALUE, MAX_VALUE);

        assertMinMaxValues(0, 42);
        assertMinMaxValues(42, 42);
        assertMinMaxValues(MIN_VALUE, 42);
        assertMinMaxValues(42, MAX_VALUE);
        assertMinMaxValues(MIN_VALUE, MAX_VALUE);

        assertValues(-42, 0, ContiguousSet.create(Range.closed(-42, 0), DiscreteDomain.integers()).asList());
        assertValues(-42, 42, ContiguousSet.create(Range.closed(-42, 42), DiscreteDomain.integers()).asList());
        assertValues(0, 42, ContiguousSet.create(Range.closed(0, 42), DiscreteDomain.integers()).asList());
        assertValues(MIN_VALUE, MIN_VALUE + 42, ContiguousSet.create(Range.closed(MIN_VALUE, MIN_VALUE + 42), DiscreteDomain.integers()).asList());
        assertValues(MAX_VALUE - 42, MAX_VALUE, ContiguousSet.create(Range.closed(MAX_VALUE - 42, MAX_VALUE), DiscreteDomain.integers()).asList());
    }

    @Test
    public void testValueOutOfRange()
    {
        try {
            new DateStatisticsBuilder().addValue(MAX_VALUE + 1L);
            fail("Expected ArithmeticException");
        }
        catch (ArithmeticException expected) {
        }

        try {
            new DateStatisticsBuilder().addValue(MIN_VALUE - 1L);
            fail("Expected ArithmeticException");
        }
        catch (ArithmeticException expected) {
        }
    }

    @Test
    public void testMinAverageValueBytes()
    {
        assertMinAverageValueBytes(0L, ImmutableList.of());
        assertMinAverageValueBytes(DATE_VALUE_BYTES, ImmutableList.of(42));
        assertMinAverageValueBytes(DATE_VALUE_BYTES, ImmutableList.of(0));
        assertMinAverageValueBytes(DATE_VALUE_BYTES, ImmutableList.of(0, 42, 42, 43));
    }
}
