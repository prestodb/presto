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

import static com.facebook.presto.operator.aggregation.AbstractVarianceAggregation.VarianceAccumulator.doubleVariance;
import static com.facebook.presto.operator.aggregation.AbstractVarianceAggregation.VarianceAccumulator.doubleVariancePopulation;
import static com.facebook.presto.operator.aggregation.AbstractVarianceAggregation.VarianceGroupedAccumulator.doubleVarianceGrouped;
import static com.facebook.presto.operator.aggregation.AbstractVarianceAggregation.VarianceGroupedAccumulator.doubleVariancePopulationGrouped;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;

/**
 * Generate the variance for a given set of values. This implements the
 * <a href="http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm">online algorithm</a>.
 */
public class DoubleVarianceAggregation
        extends AbstractVarianceAggregation
{
    public static final DoubleVarianceAggregation VARIANCE_INSTANCE = new DoubleVarianceAggregation(false);
    public static final DoubleVarianceAggregation VARIANCE_POP_INSTANCE = new DoubleVarianceAggregation(true);

    DoubleVarianceAggregation(boolean population)
    {
        super(population, DOUBLE);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(int valueChannel)
    {
        if (population) {
            return doubleVariancePopulationGrouped(valueChannel);
        }
        else {
            return doubleVarianceGrouped(valueChannel);
        }
    }

    @Override
    protected Accumulator createAccumulator(int valueChannel)
    {
        if (population) {
            return doubleVariancePopulation(valueChannel);
        }
        else {
            return doubleVariance(valueChannel);
        }
    }
}
