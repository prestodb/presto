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

package com.facebook.presto.hive.functions.aggregation;

import com.facebook.presto.spi.function.AccumulatorFunctions;
import com.facebook.presto.spi.function.AccumulatorStateDescription;
import com.facebook.presto.spi.function.AggregationFunctionDescription;
import com.facebook.presto.spi.function.ExternalAggregationFunctionImplementation;

import static java.util.Objects.requireNonNull;

public class HiveAggregationFunctionImplementation
        implements ExternalAggregationFunctionImplementation
{
    private final HiveAggregationFunctionDescription aggregationFunctionDescription;
    private final HiveAccumulatorFunctions methods;
    private final HiveAccumulatorStateDescription stateMetadata;

    public HiveAggregationFunctionImplementation(
            HiveAggregationFunctionDescription aggregationFunctionDescription,
            HiveAccumulatorFunctions accumulatorFunctions,
            HiveAccumulatorStateDescription accumulatorStateDescription)
    {
        this.aggregationFunctionDescription = requireNonNull(aggregationFunctionDescription);
        this.methods = requireNonNull(accumulatorFunctions);
        this.stateMetadata = requireNonNull(accumulatorStateDescription);
    }

    @Override
    public AggregationFunctionDescription getAggregationFunctionDescription()
    {
        return aggregationFunctionDescription;
    }

    @Override
    public AccumulatorFunctions getAccumulatorFunctions()
    {
        return methods;
    }

    @Override
    public AccumulatorStateDescription getAccumulatorStateDescription()
    {
        return stateMetadata;
    }
}
