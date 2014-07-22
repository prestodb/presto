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
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class GenericAggregationFunctionFactory
        implements FunctionFactory
{
    private final List<FunctionInfo> aggregations;

    public static GenericAggregationFunctionFactory fromAggregationDefinition(Class<?> clazz)
    {
        AggregationFunction metadata = clazz.getAnnotation(AggregationFunction.class);
        ApproximateAggregationFunction approximateMetadata = clazz.getAnnotation(ApproximateAggregationFunction.class);
        checkArgument(metadata != null || approximateMetadata != null, "Aggregation function annotation is missing");
        checkArgument(metadata == null || approximateMetadata == null, "Aggregation function cannot be exact and approximate");

        String name;
        if (metadata != null) {
            name = metadata.value();
        }
        else {
            name = approximateMetadata.value();
        }

        FunctionRegistry.FunctionListBuilder builder = new FunctionRegistry.FunctionListBuilder();
        for (InternalAggregationFunction aggregation : new AggregationCompiler().generateAggregationFunctions(clazz)) {
            builder.aggregate(name, aggregation);
        }

        return new GenericAggregationFunctionFactory(builder.getFunctions());
    }

    private GenericAggregationFunctionFactory(List<FunctionInfo> aggregations)
    {
        this.aggregations = ImmutableList.copyOf(checkNotNull(aggregations, "aggregations is null"));
    }

    @Override
    public List<FunctionInfo> listFunctions()
    {
        return aggregations;
    }
}
