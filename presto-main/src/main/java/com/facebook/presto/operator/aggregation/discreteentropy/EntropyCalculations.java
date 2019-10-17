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
package com.facebook.presto.operator.aggregation.discreteentropy;

public class EntropyCalculations
{
    private EntropyCalculations() {}

    public static double calculateEntropy(int[] counts)
    {
        double sum = 0.0;
        for (int i = 0; i < counts.length; ++i) {
            sum += counts[i];
        }
        if (sum == 0) {
            return 0.0;
        }
        double entropy = 0;
        for (int i = 0; i < counts.length; ++i) {
            if (counts[i] > 0) {
                entropy += (counts[i] / sum) * Math.log(sum / counts[i]);
            }
        }
        return Math.max(entropy, 0.0) / Math.log(2);
    }

    public static double calculateEntropy(double[] weights)
    {
        double sum = 0.0;
        for (int i = 0; i < weights.length; ++i) {
            sum += weights[i];
        }
        if (sum == 0) {
            return 0.0;
        }
        double entropy = 0;
        for (int i = 0; i < weights.length; ++i) {
            if (weights[i] > 0) {
                entropy += (weights[i] / sum) * Math.log(sum / weights[i]);
            }
        }
        return Math.max(entropy, 0.0) / Math.log(2);
    }

    public static double calculateEntropy(double[] weights, int[] counts)
    {
        double sum = 0.0;
        for (int i = 0; i < weights.length; ++i) {
            sum += weights[i] * counts[i];
        }
        if (sum == 0) {
            return 0.0;
        }
        double entropy = 0;
        for (int i = 0; i < weights.length; ++i) {
            if (weights[i] * counts[i] > 0) {
                entropy += (weights[i] * counts[i] / sum) * Math.log(sum / (weights[i] * counts[i]));
            }
        }
        return Math.max(entropy, 0.0) / Math.log(2);
    }

    public static double getHoldOutEntropy(
            long n,
            double sumW,
            double sumWeightLogWeight,
            double bucketWeight,
            double entryWeight,
            long entryMultiplicity)
    {
        double holdoutBucketWeight = Math.max(bucketWeight - entryWeight, 0);
        double holdoutSumWeight =
                sumW - bucketWeight + holdoutBucketWeight;
        double holdoutSumWeightLogWeight =
                sumWeightLogWeight - getXLogX(bucketWeight) + getXLogX(holdoutBucketWeight);
        double holdoutEntropy = entryMultiplicity * (n - 1) *
                calculateEntropyFromAggregates(holdoutSumWeight, holdoutSumWeightLogWeight) / n;
        return holdoutEntropy;
    }

    private static double getXLogX(double x)
    {
        return x <= 0.0 ? 0.0 : x * Math.log(x);
    }

    public static double calculateEntropyFromAggregates(double sumWeight, double sumWeightLogWeight)
    {
        if (sumWeight <= 0) {
            return 0.0;
        }
        return Math.max((Math.log(sumWeight) - sumWeightLogWeight / sumWeight) / Math.log(2.0), 0.0);
    }
}
