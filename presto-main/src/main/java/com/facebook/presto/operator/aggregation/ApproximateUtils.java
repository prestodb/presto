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

import com.google.common.base.Throwables;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.exception.OutOfRangeException;

public final class ApproximateUtils
{
    private static final NormalDistribution NORMAL_DISTRIBUTION = new NormalDistribution();

    private ApproximateUtils()
    {
    }

    public static String formatApproximateResult(double mean, double error, double confidence, boolean integral)
    {
        double zScore;
        try {
            zScore = NORMAL_DISTRIBUTION.inverseCumulativeProbability((1 + confidence) / 2);
        }
        catch (OutOfRangeException e) {
            throw Throwables.propagate(e);
        }

        StringBuilder builder = new StringBuilder();
        if (integral) {
            builder.append((long) mean);
        }
        else {
            builder.append(mean);
        }
        builder.append(" +/- ");
        if (integral) {
            builder.append((long) Math.ceil(zScore * error));
        }
        else {
            builder.append(zScore * error);
        }
        return builder.toString();
    }

    public static double countError(long samples, long count)
    {
        if (count == 0) {
            return Double.POSITIVE_INFINITY;
        }

        double p = samples / (double) count;
        double error = 1 / p * Math.sqrt(samples * (1 - p));
        return conservativeError(error, p, samples);
    }

    public static double sumError(long samples, long count, double sum, double variance)
    {
        if (count == 0) {
            return Double.POSITIVE_INFINITY;
        }

        double p = samples / (double) count;
        double mean = sum / (double) count;
        double error = 1 / p * Math.sqrt(variance / count + (1 - p) * mean * mean);
        return conservativeError(error, p, samples);
    }

    private static double conservativeError(double error, double p, double samples)
    {
        // Heuristic to determine that the sample is too small
        if (p < 0.01 && samples < 100) {
            return Double.POSITIVE_INFINITY;
        }
        return error;
    }
}
