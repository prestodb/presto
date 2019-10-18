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

import java.util.Arrays;

import static com.google.common.base.Verify.verify;
import static java.lang.Math.toIntExact;

public class EntropyCalculations
{
    private EntropyCalculations() {}

    /**
     * @implNote Based on Alizadeh Noughabi, Hadi & Arghami, N. (2010). "A New Estimator of Entropy".
     */
    public static double calculateFromSamplesUsingVasicek(double[] samples)
    {
        if (samples.length == 0) {
            return Double.NaN;
        }

        Arrays.sort(samples);
        int n = samples.length;
        int m = toIntExact(Math.max(Math.round(Math.sqrt(n)), 2));
        double entropy = 0;
        for (int i = 0; i < n; i++) {
            double sIPlusM = i + m < n ? samples[i + m] : samples[n - 1];
            double sIMinusM = i - m > 0 ? samples[i - m] : samples[0];
            double aI = i + m < n && i - m > 0 ? 2 : 1;
            entropy += Math.log(n / (aI * m) * (sIPlusM - sIMinusM));
        }
        return entropy / n / Math.log(2);
    }

    static double calculateEntropyFromHistogramAggregates(double width, double sumWeight, double sumWeightLogWeight)
    {
        verify(sumWeight > 0.0);
        return Math.max((Math.log(width * sumWeight) - sumWeightLogWeight / sumWeight) / Math.log(2.0), 0.0);
    }
}
