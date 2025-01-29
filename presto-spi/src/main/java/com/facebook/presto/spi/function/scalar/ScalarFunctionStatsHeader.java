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
package com.facebook.presto.spi.function.scalar;

import java.util.Map;

import static java.lang.String.format;

public class ScalarFunctionStatsHeader
{
    private final Map<Integer, ScalarPropagateSourceStats> argumentSourceStatsResolver;
    private final double min;
    private final double max;
    private final double distinctValuesCount;
    private final double nullFraction;
    private final double avgRowSize;

    private ScalarFunctionStatsHeader(Map<Integer, ScalarPropagateSourceStats> argumentSourceStatsResolver,
            double min,
            double max,
            double distinctValuesCount,
            double nullFraction,
            double avgRowSize)
    {
        this.argumentSourceStatsResolver = argumentSourceStatsResolver;
        this.min = min;
        this.max = max;
        this.distinctValuesCount = distinctValuesCount;
        this.nullFraction = nullFraction;
        this.avgRowSize = avgRowSize;
    }

    public ScalarFunctionStatsHeader(Map<Integer, ScalarPropagateSourceStats> argumentSourceStatsResolver, ScalarFunctionConstantStats constantStatsResolver)
    {
        this(argumentSourceStatsResolver,
                constantStatsResolver.minValue(),
                constantStatsResolver.maxValue(),
                constantStatsResolver.distinctValuesCount(),
                constantStatsResolver.nullFraction(),
                constantStatsResolver.avgRowSize());
    }

    public ScalarFunctionStatsHeader(Map<Integer, ScalarPropagateSourceStats> argumentSourceStatsResolver)
    {
        this(argumentSourceStatsResolver, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN, Double.NaN, Double.NaN);
    }

    @Override
    public String toString()
    {
        return format("argumentSourceStatsResolver: %s, min: %g, max: %g, distinctValuesCount: %g , nullFraction: %g, avgRowSize: %g",
                "{" + argumentSourceStatsResolver.entrySet().stream().map(entry -> entry.getKey() + " -> " + entry.getValue()).reduce((row, y) -> row + ", " + y) + "}",
                min, max, distinctValuesCount, nullFraction, avgRowSize);
    }

    /*
     * Get stats annotation for each of the scalar function argument, where key is the index of the position
     * of functions' argument and value is the ScalarPropagateSourceStats annotation.
     */
    public Map<Integer, ScalarPropagateSourceStats> getArgumentSourceStatsResolver()
    {
        return argumentSourceStatsResolver;
    }

    public double getMin()
    {
        return min;
    }

    public double getMax()
    {
        return max;
    }

    public double getAvgRowSize()
    {
        return avgRowSize;
    }

    public double getNullFraction()
    {
        return nullFraction;
    }

    public double getDistinctValuesCount()
    {
        return distinctValuesCount;
    }
}
