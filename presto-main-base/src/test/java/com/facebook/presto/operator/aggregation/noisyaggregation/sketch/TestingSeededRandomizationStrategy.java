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
package com.facebook.presto.operator.aggregation.noisyaggregation.sketch;

import java.util.Random;

/**
 * Seeded random numbers for testing
 */
public class TestingSeededRandomizationStrategy
        extends RandomizationStrategy
{
    private final Random random;

    public TestingSeededRandomizationStrategy(long seed)
    {
        this.random = new Random(seed);
    }

    public double nextDouble()
    {
        return random.nextDouble();
    }
}
