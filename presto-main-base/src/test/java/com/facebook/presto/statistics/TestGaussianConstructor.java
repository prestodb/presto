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

import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.testing.LocalQueryRunner;
import io.airlift.slice.Slices;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestGaussianConstructor
{
    private static final String UNIT_2D = "gaussian(ARRAY[1.0, 2.0], ARRAY[ARRAY[4.0, 0.5], ARRAY[0.5, 9.0]])";

    private LocalQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
    {
        queryRunner = new LocalQueryRunner(TEST_SESSION);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }

    @Test
    public void testBuildsTheDeclaredDistribution()
    {
        Gaussian gaussian = built(UNIT_2D);

        assertEquals(gaussian.getDimension(), 2);
        assertEquals(gaussian.getMean(), new double[] {1.0, 2.0});
        assertEquals(gaussian.getCovarianceUpper(), new double[] {4.0, 0.5, 9.0});
    }

    /**
     * Omitting the weight is the same as declaring one observation, matching
     * {@code confidence_ellipse(x, y)} being the same as a per-point weight of 1.
     */
    @Test
    public void testWeightSumDefaultsToOne()
    {
        assertEquals(built(UNIT_2D).getWeightSum(), 1.0);
        assertEquals(built("gaussian(ARRAY[0.0], ARRAY[ARRAY[1.0]], 40.0)").getWeightSum(), 40.0);
    }

    @Test
    public void testWorksAtAnyDimensionality()
    {
        assertEquals(built("gaussian(ARRAY[7.0], ARRAY[ARRAY[9.0]])").getDimension(), 1);

        Gaussian threeDimensional = built(
                "gaussian(ARRAY[1.0, 2.0, 3.0], ARRAY[ARRAY[4.0, 0.0, 0.0], ARRAY[0.0, 9.0, 0.0], ARRAY[0.0, 0.0, 16.0]])");
        assertEquals(threeDimensional.getDimension(), 3);
        assertEquals(threeDimensional.getCovarianceUpper(), new double[] {4.0, 0.0, 0.0, 9.0, 0.0, 16.0});
    }

    /**
     * The accessors read back what the constructor was given, so a distribution can be
     * taken apart and rebuilt.
     */
    @Test
    public void testRoundTripsThroughItsOwnSerialization()
    {
        Gaussian gaussian = built(UNIT_2D);

        assertEquals(Gaussian.deserialize(gaussian.serialize()), gaussian);
    }

    /**
     * A covariance computed numerically can differ across the diagonal in the last bits,
     * which must not be rejected as asymmetric.
     */
    @Test
    public void testToleratesRoundingAcrossTheDiagonal()
    {
        Gaussian gaussian = built("gaussian(ARRAY[0.0, 0.0], ARRAY[ARRAY[4.0, 0.30000000000000004], ARRAY[0.3, 9.0]])");

        assertEquals(gaussian.getCovariance(0, 1), 0.3, 1e-15);
        assertEquals(gaussian.getCovariance(0, 1), gaussian.getCovariance(1, 0));
    }

    @Test
    public void testRejectsAsymmetricCovariance()
    {
        assertFails(
                "gaussian(ARRAY[0.0, 0.0], ARRAY[ARRAY[4.0, 1.0], ARRAY[2.0, 9.0]])",
                "covariance must be symmetric");
    }

    /**
     * The symmetry test scales with the entries being compared, so a matrix whose halves
     * disagree in the first digit is asymmetric however small its entries are. Against an
     * absolute tolerance this one reads as symmetric.
     */
    @Test
    public void testRejectsAsymmetryAtSmallScales()
    {
        assertFails(
                "gaussian(ARRAY[0.0, 0.0], ARRAY[ARRAY[1e-15, 3e-16], ARRAY[6e-16, 1e-15]])",
                "covariance must be symmetric");
    }

    /**
     * Variances of 1e-15 describe a distribution a few nanometres across, which is a
     * distribution. An absolute floor on the Cholesky pivots would reject it.
     */
    @Test
    public void testAcceptsVerySmallCovariance()
    {
        Gaussian gaussian = built("gaussian(ARRAY[0.0, 0.0], ARRAY[ARRAY[1e-15, 0.0], ARRAY[0.0, 4e-15]])");

        assertEquals(gaussian.getCovarianceUpper(), new double[] {1e-15, 0.0, 4e-15});
    }

    /**
     * Entries near the top of the range must survive being averaged across the diagonal.
     */
    @Test
    public void testAcceptsVeryLargeCovariance()
    {
        Gaussian gaussian = built("gaussian(ARRAY[0.0, 0.0], ARRAY[ARRAY[1e308, 0.0], ARRAY[0.0, 1e308]])");

        assertEquals(gaussian.getCovarianceUpper(), new double[] {1e308, 0.0, 1e308});
    }

    /**
     * Averaging the halves of a near-degenerate matrix can move it across the line, so the
     * definiteness test has to see the averaged matrix rather than the one supplied. Here
     * the two halves differ by less than the symmetry tolerance, each is definite on its
     * own, and their average is not.
     */
    @Test
    public void testChecksDefinitenessOfWhatItStores()
    {
        assertFails(
                "gaussian(ARRAY[0.0, 0.0], ARRAY[ARRAY[1.0, 1.0], ARRAY[0.9999999999992, 1.0]])",
                "covariance must be positive definite");
    }

    @Test
    public void testRejectsDegenerateCovariance()
    {
        // variances of 1 with a covariance of 1 is a line, not a distribution with area
        assertFails(
                "gaussian(ARRAY[0.0, 0.0], ARRAY[ARRAY[1.0, 1.0], ARRAY[1.0, 1.0]])",
                "covariance must be positive definite");
        assertFails(
                "gaussian(ARRAY[0.0, 0.0], ARRAY[ARRAY[0.0, 0.0], ARRAY[0.0, 1.0]])",
                "covariance must be positive definite");
    }

    @Test
    public void testRejectsNegativeVariance()
    {
        assertFails(
                "gaussian(ARRAY[0.0], ARRAY[ARRAY[-1.0]])",
                "covariance must be positive definite");
    }

    @Test
    public void testRejectsShapeMismatch()
    {
        assertFails(
                "gaussian(ARRAY[0.0, 0.0], ARRAY[ARRAY[1.0]])",
                "covariance must have one row per element of mean");
        assertFails(
                "gaussian(ARRAY[0.0, 0.0], ARRAY[ARRAY[1.0], ARRAY[0.0, 1.0]])",
                "covariance must be square");
        assertFails(
                "gaussian(ARRAY[], ARRAY[])",
                "mean must have between 1 and 127 elements");
    }

    @Test
    public void testRejectsNullsAndNonFiniteValues()
    {
        assertFails("gaussian(ARRAY[0.0, null], ARRAY[ARRAY[1.0, 0.0], ARRAY[0.0, 1.0]])", "mean must not contain nulls");
        assertFails("gaussian(ARRAY[0.0], ARRAY[ARRAY[null]])", "covariance must not contain nulls");
        assertFails("gaussian(ARRAY[nan()], ARRAY[ARRAY[1.0]])", "mean must be finite");
        assertFails("gaussian(ARRAY[0.0], ARRAY[ARRAY[infinity()]])", "covariance must be finite");
    }

    @Test
    public void testRejectsUnusableWeightSum()
    {
        assertFails("gaussian(ARRAY[0.0], ARRAY[ARRAY[1.0]], -1.0)", "weight sum must be finite and not negative");
        assertFails("gaussian(ARRAY[0.0], ARRAY[ARRAY[1.0]], nan())", "weight sum must be finite and not negative");
    }

    private Gaussian built(String expression)
    {
        SqlVarbinary serialized = (SqlVarbinary) queryRunner
                .execute("SELECT CAST(" + expression + " AS VARBINARY)")
                .getOnlyValue();

        return Gaussian.deserialize(Slices.wrappedBuffer(serialized.getBytes()));
    }

    private void assertFails(String expression, String messageFragment)
    {
        try {
            built(expression);
            fail("expected " + expression + " to fail");
        }
        catch (RuntimeException e) {
            assertTrue(
                    e.getMessage() != null && e.getMessage().contains(messageFragment),
                    "unexpected message: " + e.getMessage());
        }
    }
}
