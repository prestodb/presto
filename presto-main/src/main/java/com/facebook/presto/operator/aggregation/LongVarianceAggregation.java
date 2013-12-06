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

import static com.facebook.presto.operator.aggregation.AbstractVarianceAggregation.VarianceAccumulator.longVariance;
import static com.facebook.presto.operator.aggregation.AbstractVarianceAggregation.VarianceAccumulator.longVariancePopulation;
import static com.facebook.presto.operator.aggregation.AbstractVarianceAggregation.VarianceGroupedAccumulator.longVarianceGrouped;
import static com.facebook.presto.operator.aggregation.AbstractVarianceAggregation.VarianceGroupedAccumulator.longVariancePopulationGrouped;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;

public class LongVarianceAggregation
        extends AbstractVarianceAggregation
{
    public static final LongVarianceAggregation VARIANCE_INSTANCE = new LongVarianceAggregation(false);
    public static final LongVarianceAggregation VARIANCE_POP_INSTANCE = new LongVarianceAggregation(true);

    LongVarianceAggregation(boolean population)
    {
        super(population, FIXED_INT_64);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(int valueChannel)
    {
        if (population) {
            return longVariancePopulationGrouped(valueChannel);
        }
        else {
            return longVarianceGrouped(valueChannel);
        }
    }

    @Override
    protected Accumulator createAccumulator(int valueChannel)
    {
        if (population) {
            return longVariancePopulation(valueChannel);
        }
        else {
            return longVariance(valueChannel);
        }
    }
}
