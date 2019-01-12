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

package io.prestosql.tests.statistics;

import static com.google.common.base.Preconditions.checkArgument;

public final class MetricComparisonStrategies
{
    private MetricComparisonStrategies() {}

    public static MetricComparisonStrategy noError()
    {
        return absoluteError(0);
    }

    public static MetricComparisonStrategy absoluteError(double error)
    {
        return absoluteError(-error, error);
    }

    public static MetricComparisonStrategy absoluteError(double minError, double maxError)
    {
        checkArgument(minError <= maxError, "minError '%s' has to be lower or equal than maxError '%s'", minError, maxError);
        return (actual, estimate) -> {
            double minEstimateValue = actual + minError;
            double maxEstimateValue = actual + maxError;
            return estimate >= minEstimateValue && estimate <= maxEstimateValue;
        };
    }

    public static MetricComparisonStrategy defaultTolerance()
    {
        return relativeError(.1);
    }

    public static MetricComparisonStrategy relativeError(double error)
    {
        return relativeError(-error, error);
    }

    public static MetricComparisonStrategy relativeError(double minError, double maxError)
    {
        checkArgument(minError <= maxError, "minError '%s' has to be lower or equal than maxError '%s'", minError, maxError);
        return (actual, estimate) -> {
            double minEstimateValue = actual * (minError + 1);
            double maxEstimateValue = actual * (maxError + 1);
            return estimate >= minEstimateValue && estimate <= maxEstimateValue;
        };
    }
}
