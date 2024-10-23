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

import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.testng.annotations.Test;

import static java.lang.Double.POSITIVE_INFINITY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestUniformHistogram
        extends TestHistogram
{
    ConnectorHistogram createHistogram()
    {
        return new UniformDistributionHistogram(0, 1);
    }

    RealDistribution getDistribution()
    {
        return new UniformRealDistribution();
    }

    @Override
    double getDistinctValues()
    {
        return 100;
    }

    @Test
    public void testInvalidConstruction()
    {
        assertThrows(IllegalArgumentException.class, () -> new UniformDistributionHistogram(2.0, 1.0));
    }

    @Test
    public void testNanRangeValues()
    {
        ConnectorHistogram hist = new UniformDistributionHistogram(Double.NaN, 2);
        assertTrue(hist.inverseCumulativeProbability(0.5).isUnknown());

        hist = new UniformDistributionHistogram(1.0, Double.NaN);
        assertTrue(hist.inverseCumulativeProbability(0.5).isUnknown());

        hist = new UniformDistributionHistogram(1.0, 2.0);
        assertEquals(hist.inverseCumulativeProbability(0.5).getValue(), 1.5);
    }

    @Test
    public void testInfiniteRangeValues()
    {
        // test low value as infinite
        ConnectorHistogram hist = new UniformDistributionHistogram(Double.NEGATIVE_INFINITY, 2);

        assertTrue(hist.inverseCumulativeProbability(0.5).isUnknown());
        assertEquals(hist.inverseCumulativeProbability(0.0), Estimate.unknown());
        assertEquals(hist.inverseCumulativeProbability(1.0).getValue(), 2.0);

        assertEquals(hist.cumulativeProbability(0.0, true), Estimate.unknown());
        assertEquals(hist.cumulativeProbability(1.0, true), Estimate.unknown());
        assertEquals(hist.cumulativeProbability(2.0, true).getValue(), 1.0);
        assertEquals(hist.cumulativeProbability(2.5, true).getValue(), 1.0);

        // test high value as infinite
        hist = new UniformDistributionHistogram(1.0, POSITIVE_INFINITY);

        assertTrue(hist.inverseCumulativeProbability(0.5).isUnknown());
        assertEquals(hist.inverseCumulativeProbability(0.0).getValue(), 1.0);
        assertEquals(hist.inverseCumulativeProbability(1.0), Estimate.unknown());

        assertEquals(hist.cumulativeProbability(0.0, true).getValue(), 0.0);
        assertEquals(hist.cumulativeProbability(1.0, true).getValue(), 0.0);
        assertEquals(hist.cumulativeProbability(1.5, true), Estimate.unknown());
    }

    @Test
    public void testSingleValueRange()
    {
        UniformDistributionHistogram hist = new UniformDistributionHistogram(1.0, 1.0);

        assertEquals(hist.inverseCumulativeProbability(0.0).getValue(), 1.0);
        assertEquals(hist.inverseCumulativeProbability(1.0).getValue(), 1.0);
        assertEquals(hist.inverseCumulativeProbability(0.5).getValue(), 1.0);

        assertEquals(hist.cumulativeProbability(0.0, true).getValue(), 0.0);
        assertEquals(hist.cumulativeProbability(0.5, true).getValue(), 0.0);
        assertEquals(hist.cumulativeProbability(1.0, true).getValue(), 1.0);
        assertEquals(hist.cumulativeProbability(1.5, true).getValue(), 1.0);
    }

    /**
     * {@link UniformDistributionHistogram} does not support the inclusive/exclusive arguments
     */
    @Override
    public void testInclusiveExclusive()
    {
    }
}
