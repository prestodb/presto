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
package com.facebook.presto.operator.aggregation;

import java.util.ArrayList;

public class TestDifferentialEntropyJacknifeAggregation
        extends TestDifferentialEntropyAggregation
{
    public TestDifferentialEntropyJacknifeAggregation()
    {
        super("fixed_histogram_jacknife");
    }

    @Override
    public Double getExpectedValue(int start, int length)
    {
        final ArrayList<Double> samples = new ArrayList<Double>();
        final ArrayList<Double> weights = new ArrayList<Double>();
        super.getSamplesAndWeights(start, length, samples, weights);
        return getEntropyFromSamplesAndWeights(samples, weights);
    }

    protected double getEntropyFromSamplesAndWeights(
            ArrayList<Double> samples,
            ArrayList<Double> weights)
    {
        double entropy = samples.size() *
                super.getEntropyFromSamplesAndWeights(samples, weights);

        for (int i = 0; i < samples.size(); ++i) {
            final ArrayList<Double> subSamples = new ArrayList<Double>(samples);
            subSamples.remove(i);
            final ArrayList<Double> subWeights = new ArrayList<Double>(weights);
            subWeights.remove(i);

            double holdOutEntropy = (samples.size() - 1) *
                    super.getEntropyFromSamplesAndWeights(subSamples, subWeights) /
                    samples.size();
            entropy -= holdOutEntropy;
        }

        return entropy;
    }
}
