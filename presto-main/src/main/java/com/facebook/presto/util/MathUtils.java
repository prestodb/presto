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

import java.util.Random;

public final class MathUtils
{
    private static final int NUMBER_OF_ESTIMATES = 50;
    private static final int NUMBER_OF_PROBES = 100;
    private static final int NUMBER_OF_VALUES = 500;
    private static final double[] COLLISION_ESTIMATES = new double[NUMBER_OF_ESTIMATES + 1];

    static {
        generateEstimates();
    }

    public static double estimateNumberOfHashCollisions(int numberOfValues, int hashSize)
    {
        if (hashSize == 0) {
            return 0d;
        }

        return COLLISION_ESTIMATES[NUMBER_OF_ESTIMATES * numberOfValues / hashSize] * numberOfValues / NUMBER_OF_VALUES;
    }

    private static void generateEstimates()
    {
        for (int i = 1; i <= NUMBER_OF_ESTIMATES; ++i) {
            int hashSize = NUMBER_OF_VALUES * NUMBER_OF_ESTIMATES / i;
            COLLISION_ESTIMATES[i] = simulate(NUMBER_OF_VALUES, hashSize);
        }
    }

    private static double simulate(int numberOfValues, int hashSize)
    {
        int collisions = 0;
        for (int i = 0; i < NUMBER_OF_PROBES; ++i) {
            collisions += simulationIteration(numberOfValues, hashSize);
        }
        return (double) collisions / NUMBER_OF_PROBES;
    }

    private static int simulationIteration(int numberOfValues, int hashSize)
    {
        int collisions = 0;
        Random random = new Random();
        boolean[] cells = new boolean[hashSize];

        for (int i = 0; i < numberOfValues; ++i) {
            int pos = random.nextInt(hashSize);
            while (cells[pos]) {
                collisions++;
                pos = (pos + 1) % hashSize;
            }
            cells[pos] = true;
        }
        return collisions;
    }

    private MathUtils() {}
}
