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
package com.facebook.presto.operator.aggregation.noisyaggregation;

import java.util.Random;

public class NoisyCountGaussianColumnAggregationUtils
{
    private NoisyCountGaussianColumnAggregationUtils()
    {
    }

    public static long computeNoisyCount(long trueCount, double noiseScale, Random random)
    {
        double noise = random.nextGaussian() * noiseScale;
        double noisyCount = trueCount + noise;
        double noisyCountFixedSign = Math.max(noisyCount, 0);  // count should always be >= 0
        return Math.round(noisyCountFixedSign);
    }
}
