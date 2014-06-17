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

import com.facebook.presto.operator.aggregation.state.VarianceState;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;

import static com.google.common.base.Preconditions.checkArgument;

public final class AggregationUtils
{
    private AggregationUtils()
    {
    }

    public static AggregationFunction createIsolatedApproximateAggregation(Class<? extends AbstractApproximateAggregationFunction> aggregationClass, Type parameterType)
    {
        Class<? extends AggregationFunction> functionClass = IsolatedClass.isolateClass(
                AggregationFunction.class,

                aggregationClass,

                AbstractApproximateAggregationFunction.class,
                SimpleAggregationFunction.class,

                AbstractApproximateAggregationFunction.GenericGroupedAccumulator.class,
                SimpleAggregationFunction.SimpleGroupedAccumulator.class,

                AbstractApproximateAggregationFunction.GenericAccumulator.class,
                SimpleAggregationFunction.SimpleAccumulator.class);

        try {
            return functionClass
                    .getConstructor(Type.class)
                    .newInstance(parameterType);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public static AggregationFunction createIsolatedAggregation(Class<? extends AggregationFunction> aggregationClass, Type parameterType)
    {
        Class<? extends AggregationFunction> functionClass = IsolatedClass.isolateClass(
                AggregationFunction.class,

                aggregationClass,

                AbstractAggregationFunction.class,
                SimpleAggregationFunction.class,

                AbstractAggregationFunction.GenericGroupedAccumulator.class,
                SimpleAggregationFunction.SimpleGroupedAccumulator.class,

                AbstractAggregationFunction.GenericAccumulator.class,
                SimpleAggregationFunction.SimpleAccumulator.class);

        try {
            return functionClass
                    .getConstructor(Type.class)
                    .newInstance(parameterType);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public static void updateVarianceState(VarianceState state, double value)
    {
        state.setCount(state.getCount() + 1);
        double delta = value - state.getMean();
        state.setMean(state.getMean() + delta / state.getCount());
        state.setM2(state.getM2() + delta * (value - state.getMean()));
    }

    public static void mergeVarianceState(VarianceState state, VarianceState otherState)
    {
        long count = otherState.getCount();
        double mean = otherState.getMean();
        double m2 = otherState.getM2();

        checkArgument(count >= 0, "count is negative");
        if (count == 0) {
            return;
        }
        long newCount = count + state.getCount();
        double newMean = ((count * mean) + (state.getCount() * state.getMean())) / (double) newCount;
        double delta = mean - state.getMean();
        double m2Delta = m2 + delta * delta * count * state.getCount() / (double) newCount;
        state.setM2(state.getM2() + m2Delta);
        state.setCount(newCount);
        state.setMean(newMean);
    }
}
