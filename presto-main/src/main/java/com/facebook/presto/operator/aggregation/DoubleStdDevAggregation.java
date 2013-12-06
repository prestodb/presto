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

import static com.facebook.presto.operator.aggregation.AbstractVarianceAggregation.VarianceAccumulator.doubleStandardDeviation;
import static com.facebook.presto.operator.aggregation.AbstractVarianceAggregation.VarianceAccumulator.doubleStandardDeviationPopulation;
import static com.facebook.presto.operator.aggregation.AbstractVarianceAggregation.VarianceGroupedAccumulator.doubleStandardDeviationGrouped;
import static com.facebook.presto.operator.aggregation.AbstractVarianceAggregation.VarianceGroupedAccumulator.doubleStandardDeviationPopulationGrouped;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;

public class DoubleStdDevAggregation
        extends AbstractVarianceAggregation
{
    public static final DoubleStdDevAggregation STDDEV_INSTANCE = new DoubleStdDevAggregation(false);
    public static final DoubleStdDevAggregation STDDEV_POP_INSTANCE = new DoubleStdDevAggregation(true);

    DoubleStdDevAggregation(boolean population)
    {
        super(population, DOUBLE);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(int valueChannel)
    {
        if (population) {
            return doubleStandardDeviationPopulationGrouped(valueChannel);
        }
        else {
            return doubleStandardDeviationGrouped(valueChannel);
        }
    }

    @Override
    protected Accumulator createAccumulator(int valueChannel)
    {
        if (population) {
            return doubleStandardDeviationPopulation(valueChannel);
        }
        else {
            return doubleStandardDeviation(valueChannel);
        }
    }
}
