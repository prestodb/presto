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
package io.prestosql.util;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static io.prestosql.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static io.prestosql.util.HashCollisionsSimulator.simulate;
import static java.lang.Math.pow;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHashCollisionsEstimator
{
    private static final int HASH_TABLE_SIZE = 10000;
    private static final double SIMULATION_ACCURACY = 0.05;
    private static final double EPSILON = 0.0001;

    @Test
    public void hashEstimatesShouldIncrease()
    {
        assertEquals(estimateNumberOfHashCollisions(0, 100), 0d);
        for (int i = 1; i <= HASH_TABLE_SIZE; ++i) {
            assertTrue(estimateNumberOfHashCollisions(i - 1, HASH_TABLE_SIZE) < estimateNumberOfHashCollisions(i, HASH_TABLE_SIZE));
        }
    }

    @Test
    public void hashEstimatesShouldScaleLinearly()
    {
        hashEstimatesShouldScaleLinearly(3000, 10000);
        hashEstimatesShouldScaleLinearly(5000, 10000);
        hashEstimatesShouldScaleLinearly(7500, 10000);
    }

    private void hashEstimatesShouldScaleLinearly(int numberOfValues, int hashTableSize)
    {
        double unscaledEstimate = estimateNumberOfHashCollisions(numberOfValues, hashTableSize);
        for (int exponent : ImmutableList.of(1, 3, 4)) {
            int scale = (int) pow(10, exponent);
            double scaledEstimate = estimateNumberOfHashCollisions(numberOfValues * scale, hashTableSize * scale);
            assertEquals(unscaledEstimate * scale, scaledEstimate, EPSILON);
        }
    }

    // Disabled by default since it takes a lot of time to execute
    @Test(enabled = false)
    public void hashEstimatesShouldApproximateSimulations()
    {
        hashEstimatesShouldApproximateSimulations(1000, 10000);
        hashEstimatesShouldApproximateSimulations(1500, 10000);
        hashEstimatesShouldApproximateSimulations(3000, 10000);
        hashEstimatesShouldApproximateSimulations(5000, 10000);
        hashEstimatesShouldApproximateSimulations(7500, 10000);
        hashEstimatesShouldApproximateSimulations(9000, 10000);
    }

    private void hashEstimatesShouldApproximateSimulations(int numberOfValues, int hashTableSize)
    {
        for (int exponent = 1; exponent <= 4; exponent++) {
            int scale = (int) pow(10, exponent);
            double scaledEstimate = estimateNumberOfHashCollisions(numberOfValues * scale, hashTableSize * scale);
            double simulatedEstimate = simulate(numberOfValues * scale, hashTableSize * scale, 2);
            assertEquals(scaledEstimate, simulatedEstimate, simulatedEstimate * SIMULATION_ACCURACY);
        }
    }
}
