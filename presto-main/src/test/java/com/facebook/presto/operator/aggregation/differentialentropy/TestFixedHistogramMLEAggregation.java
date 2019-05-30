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

public class TestFixedHistogramMLEAggregation
        extends TestFixedHistogramAggregation
{
    public TestFixedHistogramMLEAggregation()
    {
        super("fixed_histogram_mle");
    }

    @Override
    public Double getExpectedValue(int start, int length)
    {
        final ArrayList<Double> samples = new ArrayList<Double>();
        final ArrayList<Double> weights = new ArrayList<Double>();
        super.getSamplesAndWeights(start, length, samples, weights);
        return super.getEntropyFromSamplesAndWeights(samples, weights);
    }
}
