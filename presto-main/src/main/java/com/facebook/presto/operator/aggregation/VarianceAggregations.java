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

import com.facebook.presto.operator.aggregation.SimpleAggregationFunction.SimpleAccumulator;
import com.facebook.presto.operator.aggregation.SimpleAggregationFunction.SimpleGroupedAccumulator;
import com.facebook.presto.operator.aggregation.VarianceAggregation.VarianceAccumulator;
import com.facebook.presto.operator.aggregation.VarianceAggregation.VarianceGroupedAccumulator;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;

import static com.facebook.presto.type.BigintType.BIGINT;
import static com.facebook.presto.type.DoubleType.DOUBLE;

public final class VarianceAggregations
{
    public static final AggregationFunction LONG_VARIANCE_INSTANCE = createIsolatedAggregation(BIGINT, false, false);
    public static final AggregationFunction LONG_VARIANCE_POP_INSTANCE = createIsolatedAggregation(BIGINT, true, false);
    public static final AggregationFunction LONG_STDDEV_INSTANCE = createIsolatedAggregation(BIGINT, false, true);
    public static final AggregationFunction LONG_STDDEV_POP_INSTANCE = createIsolatedAggregation(BIGINT, true, true);
    public static final AggregationFunction DOUBLE_VARIANCE_INSTANCE = createIsolatedAggregation(DOUBLE, false, false);
    public static final AggregationFunction DOUBLE_VARIANCE_POP_INSTANCE = createIsolatedAggregation(DOUBLE, true, false);
    public static final AggregationFunction DOUBLE_STDDEV_INSTANCE = createIsolatedAggregation(DOUBLE, false, true);
    public static final AggregationFunction DOUBLE_STDDEV_POP_INSTANCE = createIsolatedAggregation(DOUBLE, true, true);

    private VarianceAggregations() {}

    private static AggregationFunction createIsolatedAggregation(Type parameterType, boolean population, boolean standardDeviation)
    {
        Class<? extends AggregationFunction> functionClass = IsolatedClass.isolateClass(
                AggregationFunction.class,

                VarianceAggregation.class,
                SimpleAggregationFunction.class,

                VarianceGroupedAccumulator.class,
                SimpleGroupedAccumulator.class,

                VarianceAccumulator.class,
                SimpleAccumulator.class);

        try {
            return functionClass
                    .getConstructor(Type.class, boolean.class, boolean.class)
                    .newInstance(parameterType, population, standardDeviation);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
