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
package com.facebook.presto.statistics;

import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static com.facebook.presto.statistics.Gaussian.FORMAT_VERSION;
import static com.facebook.presto.statistics.Gaussian.covarianceEntryCount;
import static com.facebook.presto.statistics.Gaussian.deserialize;
import static com.facebook.presto.statistics.Gaussian.serializedLength;
import static org.testng.Assert.assertEquals;

public class TestGaussian
{
    @Test
    public void testRoundTrip()
    {
        Gaussian gaussian = new Gaussian(12.5, new double[] {1.0, -2.0}, new double[] {3.0, 0.5, 4.0});
        Gaussian roundTripped = deserialize(gaussian.serialize());

        assertEquals(roundTripped, gaussian);
        assertEquals(roundTripped.getDimension(), 2);
        assertEquals(roundTripped.getWeightSum(), 12.5);
        assertEquals(roundTripped.getMean(), new double[] {1.0, -2.0});
        assertEquals(roundTripped.getCovarianceUpper(), new double[] {3.0, 0.5, 4.0});
    }

    @Test
    public void testRoundTripPreservesNonFiniteValues()
    {
        Gaussian gaussian = new Gaussian(Double.POSITIVE_INFINITY, new double[] {Double.NaN}, new double[] {Double.NEGATIVE_INFINITY});
        Gaussian roundTripped = deserialize(gaussian.serialize());

        assertEquals(roundTripped.getWeightSum(), Double.POSITIVE_INFINITY);
        assertEquals(roundTripped.getMean(), new double[] {Double.NaN});
        assertEquals(roundTripped.getCovarianceUpper(), new double[] {Double.NEGATIVE_INFINITY});
    }

    @Test
    public void testCovarianceIsSymmetric()
    {
        // upper triangle {1, 2, 3, 4, 5, 6} is the matrix [[1, 2, 3], [2, 4, 5], [3, 5, 6]]
        Gaussian gaussian = new Gaussian(1.0, new double[3], new double[] {1.0, 2.0, 3.0, 4.0, 5.0, 6.0});

        assertEquals(gaussian.getCovariance(0, 0), 1.0);
        assertEquals(gaussian.getCovariance(1, 1), 4.0);
        assertEquals(gaussian.getCovariance(2, 2), 6.0);

        assertEquals(gaussian.getCovariance(0, 1), 2.0);
        assertEquals(gaussian.getCovariance(1, 0), 2.0);
        assertEquals(gaussian.getCovariance(0, 2), 3.0);
        assertEquals(gaussian.getCovariance(2, 0), 3.0);
        assertEquals(gaussian.getCovariance(1, 2), 5.0);
        assertEquals(gaussian.getCovariance(2, 1), 5.0);
    }

    @Test
    public void testDimensionality()
    {
        for (int dimension = 1; dimension <= 8; dimension++) {
            Gaussian gaussian = new Gaussian(1.0, new double[dimension], new double[covarianceEntryCount(dimension)]);
            Slice serialized = gaussian.serialize();

            assertEquals(serialized.length(), serializedLength(dimension));
            assertEquals(deserialize(serialized), gaussian);
        }
    }

    @Test
    public void testDoesNotAliasCallerArrays()
    {
        double[] mean = {1.0};
        double[] covarianceUpper = {2.0};
        Gaussian gaussian = new Gaussian(1.0, mean, covarianceUpper);

        mean[0] = 100.0;
        covarianceUpper[0] = 200.0;
        gaussian.getMean()[0] = 300.0;
        gaussian.getCovarianceUpper()[0] = 400.0;

        assertEquals(gaussian.getMean(), new double[] {1.0});
        assertEquals(gaussian.getCovarianceUpper(), new double[] {2.0});
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "dimensionality must be between 1 and 127, found 0")
    public void testRejectsEmptyMean()
    {
        new Gaussian(1.0, new double[0], new double[0]);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "dimensionality 2 requires 3 covariance entries, found 4")
    public void testRejectsCovarianceOfWrongLength()
    {
        new Gaussian(1.0, new double[2], new double[4]);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Gaussian is truncated")
    public void testRejectsTruncatedValue()
    {
        deserialize(Slices.allocate(4));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Unsupported Gaussian format version: 2")
    public void testRejectsUnknownVersion()
    {
        Slice serialized = new Gaussian(1.0, new double[1], new double[1]).serialize();
        serialized.setByte(0, FORMAT_VERSION + 1);

        deserialize(serialized);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Gaussian dimensionality must be at least 1, found 0")
    public void testRejectsZeroDimension()
    {
        Slice serialized = new Gaussian(1.0, new double[1], new double[1]).serialize();
        serialized.setByte(1, 0);

        deserialize(serialized);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Gaussian of dimensionality 2 must be 50 bytes, found 26")
    public void testRejectsLengthInconsistentWithDimension()
    {
        Slice serialized = new Gaussian(1.0, new double[1], new double[1]).serialize();
        serialized.setByte(1, 2);

        deserialize(serialized);
    }
}
