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

    /**
     * Computes the standard deviation for the random variable C = sum(1 / p * Bern(p))
     * <br /><br />
     * Derivation:
     * <pre>
     * Var(C) = Var(sum(1 / p * Bern(p)))
     *        = sum(Var(1 / p * Bern(p)))   [Bienayme formula]
     *        = n * Var(1 / p * Bern(p))    [Bern(p) are iid]
     *        = n * 1 / p^2 * Var(Bern(p))  [1 / p is constant]
     *        = n * 1 / p^2 * p * (1 - p)   [Variance of a Bernoulli distribution]
     *        = n * (1 - p) / p
     *        = samples / p * (1 - p) / p   [samples = n * p, since it's only the observed rows]
     * </pre>
     * Therefore Stddev(C) = 1 / p * sqrt(samples * (1 - p))
     */
    public static double countError(long samples, long count)
    {
        if (count == 0) {
            return Double.POSITIVE_INFINITY;
        }

        double p = samples / (double) count;
        double error = 1 / p * Math.sqrt(samples * (1 - p));
        return conservativeError(error, p, samples);
    }

    /**
     * Computes the standard deviation for the random variable S = sum(1 / p * X * Bern(p))
     * <br /><br />
     * Derivation:
     * <pre>
     * Var(S) = Var(sum(1 / p * X * Bern(p)))
     *        = sum(Var(1 / p * X * Bern(p)))                                                           [Bienayme formula]
     *        = n * Var(1 / p * X * Bern(p))                                                            [X * Bern(p) are iid]
     *        = n * 1 / p^2 * Var(X * Bern(p))                                                          [1 / p is constant]
     *        = n * 1 / p^2 * (Var(X) * Var(Bern(p)) + E(X)^2 * Var(Bern(p)) + Var(X) * E(Bern(p))^2    [Product of independent variables]
     *        = n * 1 / p^2 * (Var(X) * p(1 - p) + E(X)^2 * p(1 - p) + Var(X) * p^2)                    [Variance of a Bernoulli distribution]
     *        = n * 1 / p * (Var(X) + E(X)^2 * (1 - p))
     *        = samples / p^2 * (Var(X) + E(X)^2 * (1 - p))                                             [samples = n * p, since it's only the observed rows]
     * </pre>
     * Therefore Stddev(S) = 1 / p * sqrt(samples * (variance + mean^2 * (1 - p)))
     */
    public static double sumError(long samples, long count, double m2, double mean)
    {
        if (count == 0) {
            return Double.POSITIVE_INFINITY;
        }

        double p = samples / (double) count;
        double variance = m2 / samples;
        double error = 1 / p * Math.sqrt(samples * (variance + mean * mean * (1 - p)));
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
