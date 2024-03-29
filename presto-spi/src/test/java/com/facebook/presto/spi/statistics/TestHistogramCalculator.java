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

package com.facebook.presto.spi.statistics;

import com.facebook.presto.common.predicate.Range;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.statistics.HistogramCalculator.calculateFilterFactor;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.testng.Assert.assertEquals;

public class TestHistogramCalculator
{
    @Test
    public void testCalculateFilterFactor()
    {
        Range zeroToTen = range(0, 10);
        Range empty = Range.range(DOUBLE, NaN, true, NaN, true);

        // Equal ranges
        assertFilterFactor(Estimate.of(1.0), zeroToTen, 10, uniformHist(0, 10), 5);
        assertFilterFactor(Estimate.of(1.0), zeroToTen, 10, uniformHist(0, 10), 20);

        // Some overlap
        assertFilterFactor(Estimate.of(0.5), range(5, 3000), 5, uniformHist(zeroToTen), 10);

        // Single value overlap
        assertFilterFactor(Estimate.of(1.0 / 10), range(3, 3), 1, uniformHist(zeroToTen), 10);
        assertFilterFactor(Estimate.of(1.0 / 10), range(10, 100), 357, uniformHist(zeroToTen), 10);

        // No overlap
        assertFilterFactor(Estimate.zero(), range(20, 30), 10, uniformHist(zeroToTen), 10);

        // Empty ranges
        assertFilterFactor(Estimate.zero(), zeroToTen, 10, uniformHist(empty), 0);
        assertFilterFactor(Estimate.zero(), empty, 0, uniformHist(zeroToTen), 10);

        // no test for (empty, empty) since any return value is correct
        assertFilterFactor(Estimate.zero(), unboundedRange(), 10, uniformHist(empty), 0);
        assertFilterFactor(Estimate.zero(), empty, 0, uniformHist(unboundedRange()), 10);

        // Unbounded (infinite), NDV-based
        assertFilterFactor(Estimate.of(0.5), unboundedRange(), 10, uniformHist(unboundedRange()), 20);
        assertFilterFactor(Estimate.of(1.0), unboundedRange(), 20, uniformHist(unboundedRange()), 10);

        // NEW TESTS (TPC-H Q2)
        // unbounded ranges
        assertFilterFactor(Estimate.of(.5), unboundedRange(), 0.5, uniformHist(unboundedRange()), NaN);
        // unbounded ranges with limited distinct values
        assertFilterFactor(Estimate.of(0.2), unboundedRange(), 1.0,
                domainConstrained(unboundedRange(), uniformHist(unboundedRange())), 5.0);
    }

    private static Range range(double low, double high)
    {
        return Range.range(DOUBLE, low, true, high, true);
    }

    private static Range unboundedRange()
    {
        return Range.all(DOUBLE);
    }

    private static void assertFilterFactor(Estimate expected, Range range, double distinctValues, ConnectorHistogram histogram, double totalDistinctValues)
    {
        assertEquals(
                calculateFilterFactor(range, distinctValues, histogram, Estimate.estimateFromDouble(totalDistinctValues), true),
                expected);
    }

    private static ConnectorHistogram uniformHist(Range range)
    {
        return uniformHist(range.getLow().getObjectValue().map(Double.class::cast).orElse(NEGATIVE_INFINITY),
                range.getHigh().getObjectValue().map(Double.class::cast).orElse(POSITIVE_INFINITY));
    }

    private static ConnectorHistogram uniformHist(double low, double high)
    {
        return new UniformDistributionHistogram(low, high);
    }

    private static ConnectorHistogram domainConstrained(Range range, ConnectorHistogram source)
    {
        return DisjointRangeDomainHistogram.addDisjunction(source, range);
    }
}
