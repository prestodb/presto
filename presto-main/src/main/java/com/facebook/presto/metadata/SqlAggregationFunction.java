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
package com.facebook.presto.metadata;

import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.util.ImmutableCollectors;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.metadata.FunctionKind.APPROXIMATE_AGGREGATE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class SqlAggregationFunction
        implements SqlFunction
{
    private final Signature signature;

    public static SqlAggregationFunction create(String name, String description, InternalAggregationFunction function)
    {
        return new SimpleSqlAggregationFunction(name, description, function);
    }

    protected SqlAggregationFunction(String name, List<TypeParameter> typeParameters, String returnType, List<String> argumentTypes)
    {
        this(name, typeParameters, returnType, argumentTypes, AGGREGATE);
    }

    protected SqlAggregationFunction(String name, List<TypeParameter> typeParameters, String returnType, List<String> argumentTypes, FunctionKind kind)
    {
        requireNonNull(name, "name is null");
        requireNonNull(typeParameters, "typeParameters is null");
        requireNonNull(returnType, "returnType is null");
        requireNonNull(argumentTypes, "argumentTypes is null");
        checkArgument(kind == AGGREGATE || kind == APPROXIMATE_AGGREGATE, "kind must be an aggregate");
        this.signature = new Signature(name, kind, ImmutableList.copyOf(typeParameters), returnType, ImmutableList.copyOf(argumentTypes), false);
    }

    @Override
    public final Signature getSignature()
    {
        return signature;
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    public abstract InternalAggregationFunction specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry);

    public static class SimpleSqlAggregationFunction
            extends SqlAggregationFunction
    {
        private final InternalAggregationFunction function;
        private final String description;

        public SimpleSqlAggregationFunction(
                String name,
                String description,
                InternalAggregationFunction function)
        {
            super(name,
                    ImmutableList.<TypeParameter>of(),
                    function.getFinalType().getTypeSignature().toString(),
                    function.getParameterTypes().stream()
                            .map(Type::getTypeSignature)
                            .map(TypeSignature::toString)
                            .collect(ImmutableCollectors.toImmutableList()),
                    function.isApproximate() ? APPROXIMATE_AGGREGATE : AGGREGATE);
            this.description = description;
            this.function = requireNonNull(function, "function is null");
        }

        @Override
        public String getDescription()
        {
            return description;
        }

        @Override
        public InternalAggregationFunction specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            return function;
        }
    }
}
