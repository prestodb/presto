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
package com.facebook.presto.spi.function;

import java.util.Map;

public class ScalarStatsHeader
{
    private final Map<Integer, ScalarPropagateSourceStats> argumentStatsResolver;
    private final double min;
    private final double max;
    private final double distinctValuesCount;
    private final double nullFraction;
    private final double avgRowSize;

    private ScalarStatsHeader(Map<Integer, ScalarPropagateSourceStats> argumentStatsResolver,
            double min,
            double max,
            double distinctValuesCount,
            double nullFraction,
            double avgRowSize)
    {
        this.min = min;
        this.max = max;
        this.argumentStatsResolver = argumentStatsResolver;
        this.distinctValuesCount = distinctValuesCount;
        this.nullFraction = nullFraction;
        this.avgRowSize = avgRowSize;
    }

    public ScalarStatsHeader(ScalarFunctionConstantStats methodConstantStats, Map<Integer, ScalarPropagateSourceStats> argumentStatsResolver)
    {
        this(argumentStatsResolver,
                methodConstantStats.minValue(),
                methodConstantStats.maxValue(),
                methodConstantStats.distinctValuesCount(),
                methodConstantStats.nullFraction(),
                methodConstantStats.avgRowSize());
    }

    public ScalarStatsHeader(Map<Integer, ScalarPropagateSourceStats> argumentStatsResolver)
    {
        this(argumentStatsResolver, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN, Double.NaN, Double.NaN);
    }

    @Override
    public String toString()
    {
        return String.format("distinctValuesCount: %g , nullFraction: %g, avgRowSize: %g, min: %g, max: %g",
                distinctValuesCount, nullFraction, avgRowSize, min, max);
    }

    /*
     * Get stats annotation for each of the scalar function argument, where key is the index of the position
     * of functions' argument and value is the ScalarPropagateSourceStats annotation.
     */
    public Map<Integer, ScalarPropagateSourceStats> getArgumentStats()
    {
        return argumentStatsResolver;
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
