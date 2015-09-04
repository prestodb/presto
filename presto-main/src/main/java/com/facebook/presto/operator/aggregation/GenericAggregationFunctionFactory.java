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

import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.ParametricFunction;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class GenericAggregationFunctionFactory
        implements FunctionFactory
{
    private final List<ParametricFunction> aggregations;

    public static GenericAggregationFunctionFactory fromAggregationDefinition(Class<?> clazz, TypeManager typeManager)
    {
        FunctionListBuilder builder = new FunctionListBuilder(typeManager);
        for (InternalAggregationFunction aggregation : new AggregationCompiler(typeManager).generateAggregationFunctions(clazz)) {
            builder.aggregate(aggregation);
        }

        return new GenericAggregationFunctionFactory(builder.getFunctions());
    }

    private GenericAggregationFunctionFactory(List<ParametricFunction> aggregations)
    {
        this.aggregations = ImmutableList.copyOf(requireNonNull(aggregations, "aggregations is null"));
    }

    @Override
    public List<ParametricFunction> listFunctions()
    {
        return aggregations;
    }
}
