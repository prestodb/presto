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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public abstract class TestFixedHistogramAggregation
        extends TestAggregation
{
    private static final double min = 0;
    private static final double max = (double) TestAggregation.SIZE;

    protected TestFixedHistogramAggregation(String method)
    {
        super(
                method,
                TestFixedHistogramAggregation.min,
                TestFixedHistogramAggregation.max);
    }

    protected double getEntropyFromSamplesAndWeights(ArrayList<Double> samples, ArrayList<Double> weights)
    {
        final double weight = weights.stream().mapToDouble(c -> c).sum();
        if (weight == 0.0) {
            return 0.0;
        }
        final Map<Double, Double> bucketWeights = new HashMap<Double, Double>();
        for (int i = 0; i < samples.size(); ++i) {
            final Double s = samples.get(i);
            final Double w = weights.get(i);
            bucketWeights.put(
                    s,
                    bucketWeights.getOrDefault(s, Double.valueOf(0.0)) + w);
        }
        final double rawEntropy = bucketWeights.values().stream()
                .mapToDouble(w -> w == 0.0 ? 0.0 : w / weight * Math.log(weight / w))
                .sum() / Math.log(2.0);
        final double width =
                (TestFixedHistogramAggregation.max - TestFixedHistogramAggregation.min) /
                        TestAggregation.SIZE;
        return rawEntropy + Math.log(width) / Math.log(2);
    }
}
