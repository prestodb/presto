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

import com.facebook.presto.operator.aggregation.AbstractTestAggregationFunction;

import static com.facebook.presto.operator.aggregation.differentialentropy.EntropyCalculations.calculateFromSamplesUsingVasicek;
import static org.testng.Assert.assertTrue;

abstract class AbstractTestReservoirAggregation
        extends AbstractTestAggregationFunction
{
    protected static final int MAX_SAMPLES = 500;

    @Override
    protected String getFunctionName()
    {
        return "differential_entropy";
    }

    @Override
    public Double getExpectedValue(int start, int length)
    {
        assertTrue(2 * length < MAX_SAMPLES);
        double[] samples = new double[2 * length];
        for (int i = 0; i < length; i++) {
            samples[i] = (double) (start + i);
            samples[i + length] = (double) (start + i);
        }
        return calculateFromSamplesUsingVasicek(samples);
    }
}
