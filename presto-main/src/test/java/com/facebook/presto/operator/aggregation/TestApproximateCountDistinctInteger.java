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

import com.facebook.presto.common.type.Type;

import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;

public class TestApproximateCountDistinctInteger
        extends AbstractTestApproximateCountDistinct
{
    @Override
    public InternalAggregationFunction getAggregationFunction()
    {
        return FUNCTION_AND_TYPE_MANAGER.getAggregateFunctionImplementation(
                FUNCTION_AND_TYPE_MANAGER.lookupFunction("approx_distinct", fromTypes(INTEGER, DOUBLE)));
    }

    @Override
    public Type getValueType()
    {
        return INTEGER;
    }

    @Override
    public Object randomValue()
    {
        return (long) ThreadLocalRandom.current().nextInt();
    }
}
