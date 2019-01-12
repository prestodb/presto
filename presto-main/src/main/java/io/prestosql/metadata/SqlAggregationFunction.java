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
package io.prestosql.metadata;

import com.google.common.collect.ImmutableList;
import io.prestosql.operator.aggregation.AggregationFromAnnotationsParser;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.metadata.FunctionKind.AGGREGATE;
import static java.util.Objects.requireNonNull;

public abstract class SqlAggregationFunction
        implements SqlFunction
{
    private final Signature signature;
    private final boolean hidden;

    public static List<SqlAggregationFunction> createFunctionByAnnotations(Class<?> aggregationDefinition)
    {
        return ImmutableList.of(AggregationFromAnnotationsParser.parseFunctionDefinition(aggregationDefinition));
    }

    public static List<SqlAggregationFunction> createFunctionsByAnnotations(Class<?> aggregationDefinition)
    {
        return AggregationFromAnnotationsParser.parseFunctionDefinitions(aggregationDefinition)
                .stream()
                .map(x -> (SqlAggregationFunction) x)
                .collect(toImmutableList());
    }

    protected SqlAggregationFunction(
            String name,
            List<TypeVariableConstraint> typeVariableConstraints,
            List<LongVariableConstraint> longVariableConstraints,
            TypeSignature returnType,
            List<TypeSignature> argumentTypes)
    {
        this(name, typeVariableConstraints, longVariableConstraints, returnType, argumentTypes, AGGREGATE);
    }

    protected SqlAggregationFunction(
            String name,
            List<TypeVariableConstraint> typeVariableConstraints,
            List<LongVariableConstraint> longVariableConstraints,
            TypeSignature returnType,
            List<TypeSignature> argumentTypes,
            FunctionKind kind)
    {
        this(createSignature(name, typeVariableConstraints, longVariableConstraints, returnType, argumentTypes, kind), false);
    }

    protected SqlAggregationFunction(Signature signature, boolean hidden)
    {
        this.signature = requireNonNull(signature, "signature is null");
        this.hidden = hidden;
    }

    private static Signature createSignature(
            String name,
            List<TypeVariableConstraint> typeVariableConstraints,
            List<LongVariableConstraint> longVariableConstraints,
            TypeSignature returnType,
            List<TypeSignature> argumentTypes,
            FunctionKind kind)
    {
        requireNonNull(name, "name is null");
        requireNonNull(typeVariableConstraints, "typeVariableConstraints is null");
        requireNonNull(longVariableConstraints, "longVariableConstraints is null");
        requireNonNull(returnType, "returnType is null");
        requireNonNull(argumentTypes, "argumentTypes is null");
        checkArgument(kind == AGGREGATE, "kind must be an aggregate");
        return new Signature(
                name,
                kind,
                ImmutableList.copyOf(typeVariableConstraints),
                ImmutableList.copyOf(longVariableConstraints),
                returnType,
                ImmutableList.copyOf(argumentTypes),
                false);
    }

    @Override
    public final Signature getSignature()
    {
        return signature;
    }

    @Override
    public boolean isHidden()
    {
        return hidden;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    public abstract InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry);
}
