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

import java.util.Random;

/**
 * Utility class used to generate hash collision estimates.
 */
public final class HashCollisionsSimulator
{
    private static final int NUMBER_OF_ESTIMATES = 500;
    private static final int NUMBER_OF_PROBES = 500;
    private static final int NUMBER_OF_VALUES = 10000;

    public static void main(String[] args)
    {
        generateEstimates();
    }

    private static void generateEstimates()
    {
        System.out.print("0.0, ");
        for (int i = 1; i <= NUMBER_OF_ESTIMATES; ++i) {
            int hashSize = NUMBER_OF_VALUES * NUMBER_OF_ESTIMATES / i;
            System.out.print(simulate(NUMBER_OF_VALUES, hashSize));
            if (i < NUMBER_OF_ESTIMATES) {
                System.out.print(", ");
            }
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

    public static double simulate(int numberOfValues, int hashSize, int iterations)
    {
        int collisions = 0;
        for (int i = 0; i < iterations; ++i) {
            collisions += simulationIteration(numberOfValues, hashSize);
        }
        return (double) collisions / iterations;
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

    private HashCollisionsSimulator() {}
}
