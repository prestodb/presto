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
import org.testng.annotations.Test;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public abstract class TestHistogram
{
    abstract ConnectorHistogram createHistogram();

    abstract RealDistribution getDistribution();

    abstract double getDistinctValues();

    @Test
    public void testInverseCumulativeProbability()
    {
        ConnectorHistogram hist = createHistogram();
        RealDistribution dist = getDistribution();
        assertThrows(IllegalArgumentException.class, () -> hist.inverseCumulativeProbability(Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> hist.inverseCumulativeProbability(-1.0));
        assertThrows(IllegalArgumentException.class, () -> hist.inverseCumulativeProbability(2.0));
        assertEquals(hist.inverseCumulativeProbability(0.0).getValue(), dist.getSupportLowerBound(), .001);
        assertEquals(hist.inverseCumulativeProbability(0.25).getValue(), dist.inverseCumulativeProbability(0.25), .001);
        assertEquals(hist.inverseCumulativeProbability(0.5).getValue(), dist.getNumericalMean(), .001);
        assertEquals(hist.inverseCumulativeProbability(1.0).getValue(), dist.getSupportUpperBound(), .001);
    }

    @Test
    public void testCumulativeProbability()
    {
        ConnectorHistogram hist = createHistogram();
        RealDistribution dist = getDistribution();

        assertTrue(hist.cumulativeProbability(Double.NaN, true).isUnknown());
        assertEquals(hist.cumulativeProbability(NEGATIVE_INFINITY, true).getValue(), 0.0, .001);
        assertEquals(hist.cumulativeProbability(NEGATIVE_INFINITY, false).getValue(), 0.0, .001);
        assertEquals(hist.cumulativeProbability(POSITIVE_INFINITY, true).getValue(), 1.0, .001);
        assertEquals(hist.cumulativeProbability(POSITIVE_INFINITY, false).getValue(), 1.0, .001);

        assertEquals(hist.cumulativeProbability(dist.getSupportLowerBound() - 1, true).getValue(), 0.0, .001);
        assertEquals(hist.cumulativeProbability(dist.getSupportLowerBound(), true).getValue(), 0.0, .001);
        assertEquals(hist.cumulativeProbability(dist.getSupportUpperBound() + 1, true).getValue(), 1.0, .001);
        assertEquals(hist.cumulativeProbability(dist.getSupportUpperBound(), true).getValue(), 1.0, .001);
        assertEquals(hist.cumulativeProbability(dist.getNumericalMean(), true).getValue(), 0.5, .001);
        for (int i = 0; i < 10; i++) {
            assertEquals(hist.cumulativeProbability(dist.inverseCumulativeProbability(0.1 * i), true).getValue(), dist.cumulativeProbability(dist.inverseCumulativeProbability(0.1 * i)), .001);
        }
    }

    @Test
    public void testInclusiveExclusive()
    {
        double ndvs = getDistinctValues();
        ConnectorHistogram hist = createHistogram();
        // test maximums
        assertEquals(hist.cumulativeProbability(hist.inverseCumulativeProbability(1.0).getValue(), false).getValue(), 1.0 - (1.0 / ndvs), .0001);
        assertEquals(hist.cumulativeProbability(hist.inverseCumulativeProbability(1.0).getValue(), true).getValue(), 1.0, .0001);

        // test minimums
        assertEquals(hist.cumulativeProbability(hist.inverseCumulativeProbability(0.0).getValue(), false).getValue(), 0.0, .0001);
        assertEquals(hist.cumulativeProbability(hist.inverseCumulativeProbability(0.0).getValue(), true).getValue(), 0.0, .0001);

        // test non-max/min
        double midPercent = hist.inverseCumulativeProbability(0.5).getValue();
        assertEquals(hist.cumulativeProbability(midPercent, true).getValue() - hist.cumulativeProbability(midPercent, false).getValue(), 1.0 / ndvs, .0001);
    }
}
