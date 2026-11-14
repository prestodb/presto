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
import com.google.common.io.BaseEncoding;
import io.airlift.slice.Slices;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static org.testng.Assert.assertEquals;

public class TestGaussianOperators
{
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
    public void testRoundTripThroughVarbinary()
    {
        assertRoundTrips(new Gaussian(10.0, new double[] {1.0, 2.0}, new double[] {3.0, 0.5, 4.0}));
    }

    @Test
    public void testRoundTripPreservesDimensionality()
    {
        assertRoundTrips(new Gaussian(3.0, new double[] {7.0}, new double[] {9.0}));
        assertRoundTrips(new Gaussian(
                2.5,
                new double[] {1.0, 2.0, 3.0},
                new double[] {4.0, 0.0, 0.0, 9.0, 0.0, 16.0}));
    }

    /**
     * The varbinary is the serialized Gaussian rather than a copy of it, so a value written
     * to a table and read back is the same bytes.
     */
    @Test
    public void testCastDoesNotChangeTheBytes()
    {
        Gaussian gaussian = new Gaussian(10.0, new double[] {1.0, 2.0}, new double[] {3.0, 0.5, 4.0});
        byte[] serialized = gaussian.serialize().getBytes();

        assertEquals(castThroughGaussian(serialized), serialized);
    }

    private void assertRoundTrips(Gaussian gaussian)
    {
        byte[] returned = castThroughGaussian(gaussian.serialize().getBytes());

        assertEquals(Gaussian.deserialize(Slices.wrappedBuffer(returned)), gaussian);
    }

    private byte[] castThroughGaussian(byte[] serialized)
    {
        String hex = BaseEncoding.base16().encode(serialized);
        SqlVarbinary returned = (SqlVarbinary) queryRunner
                .execute("SELECT CAST(CAST(from_hex('" + hex + "') AS Gaussian) AS VARBINARY)")
                .getOnlyValue();

        return returned.getBytes();
    }
}
