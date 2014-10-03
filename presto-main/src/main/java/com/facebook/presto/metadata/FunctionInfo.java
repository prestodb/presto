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

import com.facebook.presto.operator.WindowFunctionDefinition;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.operator.window.WindowFunctionSupplier;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.operator.WindowFunctionDefinition.window;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public final class FunctionInfo
        implements ParametricFunction
{
    private final Signature signature;
    private final String description;
    private final boolean hidden;
    private final boolean nullable;
    private final List<Boolean> nullableArguments;

    private final boolean isAggregate;
    private final String intermediateType;
    private final InternalAggregationFunction aggregationFunction;
    private final boolean isApproximate;

    private final MethodHandle methodHandle;
    private final boolean deterministic;

    private final boolean isWindow;
    private final WindowFunctionSupplier windowFunctionSupplier;

    public FunctionInfo(Signature signature, String description, WindowFunctionSupplier windowFunctionSupplier)
    {
        this.signature = signature;
        this.description = description;
        this.hidden = false;
        this.deterministic = true;
        this.nullable = false;
        this.nullableArguments = ImmutableList.copyOf(Collections.nCopies(signature.getArgumentTypes().size(), false));

        this.isAggregate = false;
        this.intermediateType = null;
        this.aggregationFunction = null;
        this.isApproximate = false;
        this.methodHandle = null;

        this.isWindow = true;
        this.windowFunctionSupplier = checkNotNull(windowFunctionSupplier, "windowFunction is null");
    }

    public FunctionInfo(Signature signature, String description, String intermediateType, InternalAggregationFunction function, boolean isApproximate)
    {
        this.signature = signature;
        this.description = description;
        this.isApproximate = isApproximate;
        this.hidden = false;
        this.intermediateType = intermediateType;
        this.aggregationFunction = function;
        this.isAggregate = true;
        this.methodHandle = null;
        this.deterministic = true;
        this.nullable = false;
        this.nullableArguments = ImmutableList.copyOf(Collections.nCopies(signature.getArgumentTypes().size(), false));
        this.isWindow = false;
        this.windowFunctionSupplier = null;
    }

    public FunctionInfo(Signature signature, String description, boolean hidden, MethodHandle function, boolean deterministic, boolean nullableResult, List<Boolean> nullableArguments)
    {
        this.signature = signature;
        this.description = description;
        this.hidden = hidden;
        this.deterministic = deterministic;
        this.nullable = nullableResult;
        this.nullableArguments = ImmutableList.copyOf(checkNotNull(nullableArguments, "nullableArguments is null"));
        checkArgument(nullableArguments.size() == signature.getArgumentTypes().size(), String.format("nullableArguments size (%d) does not match signature %s", nullableArguments.size(), signature));

        this.isAggregate = false;
        this.intermediateType = null;
        this.aggregationFunction = null;
        this.isApproximate = false;

        this.isWindow = false;
        this.windowFunctionSupplier = null;
        this.methodHandle = checkNotNull(function, "function is null");
    }

    @Override
    public Signature getSignature()
    {
        return signature;
    }

    public QualifiedName getName()
    {
        return QualifiedName.of(signature.getName());
    }

    @Override
    public String getDescription()
    {
        return description;
    }

    @Override
    public boolean isHidden()
    {
        return hidden;
    }

    @Override
    public boolean isAggregate()
    {
        return isAggregate;
    }

    @Override
    public boolean isWindow()
    {
        return isWindow;
    }

    @Override
    public boolean isScalar()
    {
        return !isWindow && !isAggregate;
    }

    @Override
    public boolean isUnbound()
    {
        return false;
    }

    @Override
    public boolean isApproximate()
    {
        return isApproximate;
    }

    public String getReturnType()
    {
        return signature.getReturnType();
    }

    public List<String> getArgumentTypes()
    {
        return signature.getArgumentTypes();
    }

    public String getIntermediateType()
    {
        return intermediateType;
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager)
    {
        return this;
    }

    public WindowFunctionDefinition bindWindowFunction(List<Integer> inputs)
    {
        checkState(isWindow, "not a window function");
        return window(windowFunctionSupplier, inputs);
    }

    public InternalAggregationFunction getAggregationFunction()
    {
        checkState(aggregationFunction != null, "not an aggregation function");
        return aggregationFunction;
    }

    public MethodHandle getMethodHandle()
    {
        checkState(methodHandle != null, "not a scalar function or operator");
        return methodHandle;
    }

    @Override
    public boolean isDeterministic()
    {
        return deterministic;
    }

    public boolean isNullable()
    {
        return nullable;
    }

    public List<Boolean> getNullableArguments()
    {
        return nullableArguments;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        FunctionInfo other = (FunctionInfo) obj;
        return Objects.equal(this.signature, other.signature) &&
                Objects.equal(this.isAggregate, other.isAggregate) &&
                Objects.equal(this.isWindow, other.isWindow);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(signature, isAggregate, isWindow);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("signature", signature)
                .add("isAggregate", isAggregate)
                .add("isWindow", isWindow)
                .toString();
    }

    public static Function<FunctionInfo, Signature> handleGetter()
    {
        return new Function<FunctionInfo, Signature>()
        {
            @Override
            public Signature apply(FunctionInfo input)
            {
                return input.getSignature();
            }
        };
    }
}
