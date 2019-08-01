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

import com.facebook.presto.spi.PrestoException;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;

/**
 * Utility class for different strategies for calculating entropy based on fixed histograms.
 */
public class FixedHistogramStateStrategyUtils
{
    private FixedHistogramStateStrategyUtils() {}

    public static void validateParameters(
            long bucketCount,
            double min,
            double max)
    {
        if (bucketCount < 0) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    format("In differential_entropy UDF, bucket count must be non-negative: %s", bucketCount));
        }

        if (min >= max) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT, format(
                            "In differential_entropy UDF, min must be larger than max: min=%s, max=%s", min, max));
        }
    }

    public static void validateParameters(
            long histogramBucketCount,
            double histogramMin,
            double histogramMax,
            long bucketCount,
            double sample,
            double weight,
            double min,
            double max)
    {
        validateParameters(histogramBucketCount, histogramMin, histogramMax);
        validateParameters(bucketCount, min, max);

        if (weight < 0.0) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    format("In differential_entropy UDF, weight must be non-negative: %s", weight));
        }

        if (histogramBucketCount != bucketCount) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    format("In differential_entropy UDF, inconsistent bucket count: prev=%s, current=%s", histogramBucketCount, bucketCount));
        }
        if (histogramMin != min) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    format("In differential_entropy UDF, inconsistent min: prev=%s, current=%s", histogramMin, min));
        }
        if (histogramMax != max) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    format("In differential_entropy UDF, inconsistent max: prev=%s, current=%s", histogramMax, max));
        }
        if (sample < min) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    format("In differential_entropy UDF, sample must be at least min: sample=%s, min=%s", sample, min));
        }
        if (sample > max) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    format("In differential_entropy UDF, sample must be at most max: sample=%s, max=%s", sample, max));
        }
    }

    public static double getXLogX(double x)
    {
        return x <= 0.0 ? 0.0 : x * Math.log(x);
    }
}
