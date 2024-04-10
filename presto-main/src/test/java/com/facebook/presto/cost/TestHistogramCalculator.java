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

package com.facebook.presto.cost;

import com.facebook.presto.spi.statistics.ConnectorHistogram;
import com.facebook.presto.spi.statistics.Estimate;
import org.testng.annotations.Test;

import static com.facebook.presto.cost.HistogramCalculator.calculateFilterFactor;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.testng.Assert.assertEquals;

public class TestHistogramCalculator
{
    @Test
    public void testCalculateFilterFactor()
    {
        StatisticRange zeroToTen = range(0, 10, 10);
        StatisticRange empty = StatisticRange.empty();

        // Equal ranges
        assertFilterFactor(Estimate.of(1.0), zeroToTen, uniformHist(0, 10), 5);
        assertFilterFactor(Estimate.of(1.0), zeroToTen, uniformHist(0, 10), 20);

        // Some overlap
        assertFilterFactor(Estimate.of(0.5), range(5, 3000, 5), uniformHist(zeroToTen), zeroToTen.getDistinctValuesCount());

        // Single value overlap
        assertFilterFactor(Estimate.of(1.0 / zeroToTen.getDistinctValuesCount()), range(3, 3, 1), uniformHist(zeroToTen), zeroToTen.getDistinctValuesCount());
        assertFilterFactor(Estimate.of(1.0 / zeroToTen.getDistinctValuesCount()), range(10, 100, 357), uniformHist(zeroToTen), zeroToTen.getDistinctValuesCount());

        // No overlap
        assertFilterFactor(Estimate.zero(), range(20, 30, 10), uniformHist(zeroToTen), zeroToTen.getDistinctValuesCount());

        // Empty ranges
        assertFilterFactor(Estimate.zero(), zeroToTen, uniformHist(empty), empty.getDistinctValuesCount());
        assertFilterFactor(Estimate.zero(), empty, uniformHist(zeroToTen), zeroToTen.getDistinctValuesCount());

        // no test for (empty, empty) since any return value is correct
        assertFilterFactor(Estimate.zero(), unboundedRange(10), uniformHist(empty), empty.getDistinctValuesCount());
        assertFilterFactor(Estimate.zero(), empty, uniformHist(unboundedRange(10)), 10);

        // Unbounded (infinite), NDV-based
        assertFilterFactor(Estimate.of(0.5), unboundedRange(10), uniformHist(unboundedRange(20)), 20);
        assertFilterFactor(Estimate.of(1.0), unboundedRange(20), uniformHist(unboundedRange(10)), 10);

        // NEW TESTS (TPC-H Q2)
        // unbounded ranges
        assertFilterFactor(Estimate.of(.5), unboundedRange(0.5), uniformHist(unboundedRange(NaN)), NaN);
        // unbounded ranges with limited distinct values
        assertFilterFactor(Estimate.of(0.2), unboundedRange(1.0),
                domainConstrained(unboundedRange(5.0), uniformHist(unboundedRange(7.0))), 5.0);
    }

    private static StatisticRange range(double low, double high, double distinctValues)
    {
        return new StatisticRange(low, high, distinctValues);
    }

    private static StatisticRange unboundedRange(double distinctValues)
    {
        return new StatisticRange(NEGATIVE_INFINITY, POSITIVE_INFINITY, distinctValues);
    }

    private static void assertFilterFactor(Estimate expected, StatisticRange range, ConnectorHistogram histogram, double totalDistinctValues)
    {
        assertEquals(
                calculateFilterFactor(range, histogram, Estimate.estimateFromDouble(totalDistinctValues), true),
                expected);
    }

    private static ConnectorHistogram uniformHist(StatisticRange range)
    {
        return uniformHist(range.getLow(), range.getHigh());
    }

    private static ConnectorHistogram uniformHist(double low, double high)
    {
        return new UniformDistributionHistogram(low, high);
    }

    private static ConnectorHistogram domainConstrained(StatisticRange range, ConnectorHistogram source)
    {
        return DisjointRangeDomainHistogram.addDisjunction(source, range);
    }
}
