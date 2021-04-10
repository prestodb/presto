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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.operator.aggregation.AggregationFromAnnotationsParser;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.LongVariableConstraint;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.spi.function.TypeVariableConstraint;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.spi.function.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public abstract class SqlAggregationFunction
        extends BuiltInFunction
{
    private final Signature signature;
    private final SqlFunctionVisibility visibility;

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
        this(createSignature(name, typeVariableConstraints, longVariableConstraints, returnType, argumentTypes, kind), PUBLIC);
    }

    protected SqlAggregationFunction(Signature signature, SqlFunctionVisibility visibility)
    {
        this.signature = requireNonNull(signature, "signature is null");
        this.visibility = visibility;
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
                QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, name),
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
    public SqlFunctionVisibility getVisibility()
    {
        return visibility;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    public abstract InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager);
}
