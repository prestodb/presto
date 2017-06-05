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

package com.facebook.presto.tests.statistics;

import com.google.common.collect.Range;

import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;

public class MetricComparisonStrategies
{
    private MetricComparisonStrategies() {}

    public static MetricComparisonStrategy<Double> noError()
    {
        return absoluteError(0);
    }

    public static MetricComparisonStrategy<Double> absoluteError(double error)
    {
        return absoluteError(Range.closed(-error, error));
    }

    public static MetricComparisonStrategy<Double> absoluteError(Range<Double> errorRange)
    {
        return (actual, estimate) -> mapRange(errorRange, endpoint -> endpoint + actual)
                .contains(estimate);
    }

    public static MetricComparisonStrategy<Double> defaultTolerance()
    {
        return relativeError(.1);
    }

    public static MetricComparisonStrategy<Double> relativeError(double error)
    {
        return relativeError(Range.closed(-error, error));
    }

    public static MetricComparisonStrategy<Double> relativeError(Range<Double> errorRange)
    {
        return (actual, estimate) -> mapRange(errorRange, endpoint -> (endpoint + 1) * actual)
                .contains(estimate);
    }

    private static Range<Double> mapRange(Range<Double> range, Function<Double, Double> mappingFunction)
    {
        checkArgument(range.hasLowerBound() && range.hasUpperBound(), "Expected error range to have lower and upper bound");
        return Range.range(
                mappingFunction.apply(range.lowerEndpoint()),
                range.lowerBoundType(),
                mappingFunction.apply(range.upperEndpoint()),
                range.upperBoundType());
    }
}
