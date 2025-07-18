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
package com.facebook.presto.operator.aggregation.arrayagg;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.BuiltInAggregationFunctionImplementation;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.function.Signature.typeVariable;

/**
 * Alias for array_agg function to match Spark SQL's collect_list
 */
public class CollectListAggregationFunction
        extends SqlAggregationFunction
{
    private static final String NAME = "collect_list";
    private final ArrayAggregationFunction delegate;

    public CollectListAggregationFunction(boolean legacyArrayAgg, ArrayAggGroupImplementation groupMode)
    {
        super(NAME,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("array(T)"),
                ImmutableList.of(parseTypeSignature("T")));
        this.delegate = new ArrayAggregationFunction(legacyArrayAgg, groupMode);
    }

    @Override
    public String getDescription()
    {
        return "return an array of values (alias for array_agg)";
    }

    @Override
    public BuiltInAggregationFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        return delegate.specialize(boundVariables, arity, functionAndTypeManager);
    }
}
