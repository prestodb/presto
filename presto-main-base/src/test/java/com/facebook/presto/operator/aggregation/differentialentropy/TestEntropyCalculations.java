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
package com.facebook.presto.operator.aggregation.differentialentropy;

import org.testng.annotations.Test;

import java.util.Random;

import static com.facebook.presto.operator.aggregation.differentialentropy.EntropyCalculations.calculateFromSamplesUsingVasicek;
import static org.testng.Assert.assertEquals;

public class TestEntropyCalculations
{
    @Test
    public void testUniformDistribution()
    {
        Random random = new Random(13);
        double[] samples = new double[10000000];
        for (int i = 0; i < samples.length; i++) {
            samples[i] = random.nextDouble();
        }
        assertEquals(calculateFromSamplesUsingVasicek(samples), 0, 0.02);
    }

    @Test
    public void testNormalDistribution()
    {
        Random random = new Random(13);
        double[] samples = new double[10000000];
        double sigma = 0.5;
        for (int i = 0; i < samples.length; i++) {
            samples[i] = 5 + sigma * random.nextGaussian();
        }
        double expected = 0.5 * Math.log(2 * Math.PI * Math.E * sigma * sigma) / Math.log(2);
        assertEquals(calculateFromSamplesUsingVasicek(samples), expected, 0.02);
    }
}
