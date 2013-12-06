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

import static com.facebook.presto.operator.aggregation.AbstractVarianceAggregation.VarianceAccumulator.longStandardDeviation;
import static com.facebook.presto.operator.aggregation.AbstractVarianceAggregation.VarianceAccumulator.longStandardDeviationPopulation;
import static com.facebook.presto.operator.aggregation.AbstractVarianceAggregation.VarianceGroupedAccumulator.longStandardDeviationGrouped;
import static com.facebook.presto.operator.aggregation.AbstractVarianceAggregation.VarianceGroupedAccumulator.longStandardDeviationPopulationGrouped;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;

public class LongStdDevAggregation
        extends AbstractVarianceAggregation
{
    public static final LongStdDevAggregation STDDEV_INSTANCE = new LongStdDevAggregation(false);
    public static final LongStdDevAggregation STDDEV_POP_INSTANCE = new LongStdDevAggregation(true);

    LongStdDevAggregation(boolean population)
    {
        super(population, FIXED_INT_64);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(int valueChannel)
    {
        if (population) {
            return longStandardDeviationPopulationGrouped(valueChannel);
        }
        else {
            return longStandardDeviationGrouped(valueChannel);
        }
    }

    @Override
    protected Accumulator createAccumulator(int valueChannel)
    {
        if (population) {
            return longStandardDeviationPopulation(valueChannel);
        }
        else {
            return longStandardDeviation(valueChannel);
        }
    }
}

