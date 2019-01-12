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
package io.prestosql.cost;

import org.testng.annotations.Test;

import static io.prestosql.cost.EstimateAssertion.assertEstimateEquals;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.testng.Assert.assertEquals;

public class TestStatisticRange
{
    @Test
    public void testOverlapPercentWith()
    {
        StatisticRange zeroToTen = range(0, 10, 10);
        StatisticRange empty = StatisticRange.empty();

        // Equal ranges
        assertOverlap(zeroToTen, range(0, 10, 5), 1);
        assertOverlap(zeroToTen, range(0, 10, 20), 1);
        assertOverlap(zeroToTen, range(0, 10, 20), 1);

        // Some overlap
        assertOverlap(zeroToTen, range(5, 3000, 3), 0.5);

        // Single value overlap
        assertOverlap(zeroToTen, range(3, 3, 1), 1 / zeroToTen.getDistinctValuesCount());
        assertOverlap(zeroToTen, range(10, 100, 357), 1 / zeroToTen.getDistinctValuesCount());

        // No overlap
        assertOverlap(zeroToTen, range(20, 30, 10), 0);

        // Empty ranges
        assertOverlap(zeroToTen, empty, 0);
        assertOverlap(empty, zeroToTen, 0);
        // no test for empty, empty) since any return value is correct
        assertOverlap(unboundedRange(10), empty, 0);

        // Unbounded (infinite), NDV-based
        assertOverlap(unboundedRange(10), unboundedRange(20), 1);
        assertOverlap(unboundedRange(20), unboundedRange(10), 0.5);

        assertOverlap(unboundedRange(0.1), unboundedRange(1), 1);
        assertOverlap(unboundedRange(0.0), unboundedRange(1), 0);
        assertOverlap(unboundedRange(0.0), unboundedRange(0), 0);
    }

    @Test
    public void testIntersect()
    {
        StatisticRange zeroToTen = range(0, 10, 10);
        StatisticRange fiveToFifteen = range(5, 15, 60);
        assertEquals(zeroToTen.intersect(fiveToFifteen), range(5, 10, 10));
    }

    @Test
    public void testAddAndSumDistinctValues()
    {
        assertEquals(unboundedRange(NaN).addAndSumDistinctValues(unboundedRange(NaN)), unboundedRange(NaN));
        assertEquals(unboundedRange(NaN).addAndSumDistinctValues(unboundedRange(1)), unboundedRange(NaN));
        assertEquals(unboundedRange(1).addAndSumDistinctValues(unboundedRange(NaN)), unboundedRange(NaN));
        assertEquals(unboundedRange(1).addAndSumDistinctValues(unboundedRange(2)), unboundedRange(3));
        assertEquals(StatisticRange.empty().addAndSumDistinctValues(StatisticRange.empty()), StatisticRange.empty());
        assertEquals(range(0, 1, 1).addAndSumDistinctValues(StatisticRange.empty()), range(0, 1, 1));
        assertEquals(range(0, 1, 1).addAndSumDistinctValues(range(1, 2, 1)), range(0, 2, 2));
    }

    @Test
    public void testAddAndMaxDistinctValues()
    {
        assertEquals(unboundedRange(NaN).addAndMaxDistinctValues(unboundedRange(NaN)), unboundedRange(NaN));
        assertEquals(unboundedRange(NaN).addAndMaxDistinctValues(unboundedRange(1)), unboundedRange(NaN));
        assertEquals(unboundedRange(1).addAndMaxDistinctValues(unboundedRange(NaN)), unboundedRange(NaN));
        assertEquals(unboundedRange(1).addAndMaxDistinctValues(unboundedRange(2)), unboundedRange(2));
        assertEquals(StatisticRange.empty().addAndMaxDistinctValues(StatisticRange.empty()), StatisticRange.empty());
        assertEquals(range(0, 1, 1).addAndMaxDistinctValues(StatisticRange.empty()), range(0, 1, 1));
        assertEquals(range(0, 1, 1).addAndMaxDistinctValues(range(1, 2, 1)), range(0, 2, 1));
    }

    @Test
    public void testAddAndCollapseDistinctValues()
    {
        assertEquals(unboundedRange(NaN).addAndCollapseDistinctValues(unboundedRange(NaN)), unboundedRange(NaN));
        assertEquals(unboundedRange(NaN).addAndCollapseDistinctValues(unboundedRange(1)), unboundedRange(NaN));
        assertEquals(unboundedRange(1).addAndCollapseDistinctValues(unboundedRange(NaN)), unboundedRange(NaN));
        assertEquals(unboundedRange(1).addAndCollapseDistinctValues(unboundedRange(2)), unboundedRange(2));
        assertEquals(StatisticRange.empty().addAndCollapseDistinctValues(StatisticRange.empty()), StatisticRange.empty());
        assertEquals(range(0, 1, 1).addAndCollapseDistinctValues(StatisticRange.empty()), range(0, 1, 1));
        assertEquals(range(0, 1, 1).addAndCollapseDistinctValues(range(1, 2, 1)), range(0, 2, 1));
        assertEquals(range(0, 3, 3).addAndCollapseDistinctValues(range(2, 6, 4)), range(0, 6, 6));
    }

    private static StatisticRange range(double low, double high, double distinctValues)
    {
        return new StatisticRange(low, high, distinctValues);
    }

    private static StatisticRange unboundedRange(double distinctValues)
    {
        return new StatisticRange(NEGATIVE_INFINITY, POSITIVE_INFINITY, distinctValues);
    }

    private static void assertOverlap(StatisticRange a, StatisticRange b, double expected)
    {
        assertEstimateEquals(a.overlapPercentWith(b), expected, "overlapPercentWith");
    }
}
