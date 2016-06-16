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
package com.facebook.presto.util;

import org.testng.annotations.Test;

import static com.facebook.presto.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static java.lang.Math.pow;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class HashCollisionsEstimatorTest
{
    private static final int HASH_TABLE_SIZE = 10000;

    @Test
    public void hashEstimatesShouldIncrease()
    {
        assertEquals(estimateNumberOfHashCollisions(0, 100), 0d);
        for (int i = 1; i <= HASH_TABLE_SIZE; ++i) {
            assertTrue(estimateNumberOfHashCollisions(i - 1, HASH_TABLE_SIZE) < estimateNumberOfHashCollisions(i, HASH_TABLE_SIZE));
        }
    }

    @Test
    public void hashEstimatesShouldScale()
    {
        for (int i = 1; i < 5; ++i) {
            int scale = (int) pow(10, i);
            assertEquals(estimateNumberOfHashCollisions(4500, 7000) * scale, estimateNumberOfHashCollisions(4500 * scale, 7000 * scale));
        }
    }
}
