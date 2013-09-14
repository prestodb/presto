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

import com.facebook.presto.operator.AggregationFunctionDefinition;
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.gen.FunctionBinder;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class FunctionInfo
        implements Comparable<FunctionInfo>
{
    private final int id;

    private final QualifiedName name;
    private final String description;
    private final Type returnType;
    private final List<Type> argumentTypes;

    private final boolean isAggregate;
    private final Type intermediateType;
    private final AggregationFunction aggregationFunction;

    private final MethodHandle scalarFunction;
    private final boolean deterministic;
    private final FunctionBinder functionBinder;

    private final boolean isWindow;
    private final Supplier<WindowFunction> windowFunction;

    public FunctionInfo(int id, QualifiedName name, String description, Type returnType, List<Type> argumentTypes, Supplier<WindowFunction> windowFunction)
    {
        this.id = id;
        this.name = name;
        this.description = description;
        this.returnType = returnType;
        this.argumentTypes = argumentTypes;
        this.deterministic = true;

        this.isAggregate = false;
        this.intermediateType = null;
        this.aggregationFunction = null;
        this.scalarFunction = null;
        this.functionBinder = null;

        this.isWindow = true;
        this.windowFunction = checkNotNull(windowFunction, "windowFunction is null");
    }

    public FunctionInfo(int id, QualifiedName name, String description, Type returnType, List<Type> argumentTypes, Type intermediateType, AggregationFunction function)
    {
        this.id = id;
        this.name = name;
        this.description = description;
        this.returnType = returnType;
        this.argumentTypes = argumentTypes;
        this.intermediateType = intermediateType;
        this.aggregationFunction = function;
        this.isAggregate = true;
        this.scalarFunction = null;
        this.deterministic = true;
        this.functionBinder = null;
        this.isWindow = false;
        this.windowFunction = null;
    }

    public FunctionInfo(int id, QualifiedName name, String description, Type returnType, List<Type> argumentTypes, MethodHandle function, boolean deterministic, FunctionBinder functionBinder)
    {
        this.id = id;
        this.name = name;
        this.description = description;
        this.returnType = returnType;
        this.argumentTypes = argumentTypes;
        this.deterministic = deterministic;
        this.functionBinder = functionBinder;

        this.isAggregate = false;
        this.intermediateType = null;
        this.aggregationFunction = null;

        this.isWindow = false;
        this.windowFunction = null;
        this.scalarFunction = checkNotNull(function, "function is null");
    }

    public FunctionHandle getHandle()
    {
        return new FunctionHandle(id, name.toString());
    }

    public QualifiedName getName()
    {
        return name;
    }

    public String getDescription()
    {
        return description;
    }

    public boolean isAggregate()
    {
        return isAggregate;
    }

    public boolean isWindow()
    {
        return isWindow;
    }

    public Supplier<WindowFunction> getWindowFunction()
    {
        checkState(isWindow, "not a window function");
        return windowFunction;
    }

    public Type getReturnType()
    {
        return returnType;
    }

    public List<Type> getArgumentTypes()
    {
        return argumentTypes;
    }

    public Type getIntermediateType()
    {
        return intermediateType;
    }

    public AggregationFunctionDefinition bind(List<Input> inputs)
    {
        checkState(isAggregate, "function is not an aggregate");
        return aggregation(aggregationFunction, inputs);
    }

    public MethodHandle getScalarFunction()
    {
        checkState(scalarFunction != null, "not a scalar function");
        return scalarFunction;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }

    public FunctionBinder getFunctionBinder()
    {
        return functionBinder;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        FunctionInfo o = (FunctionInfo) obj;
        return Objects.equal(isWindow, o.isWindow) &&
                Objects.equal(isAggregate, o.isAggregate) &&
                Objects.equal(name, o.name) &&
                Objects.equal(argumentTypes, o.argumentTypes) &&
                Objects.equal(returnType, o.returnType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(isWindow, isAggregate, name, argumentTypes, returnType);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("isAggregate", isAggregate)
                .add("isWindow", isWindow)
                .add("name", name)
                .add("argumentTypes", argumentTypes)
                .add("returnType", returnType)
                .toString();
    }

    @Override
    public int compareTo(FunctionInfo o)
    {
        return ComparisonChain.start()
                .compareTrueFirst(isWindow, o.isWindow)
                .compareTrueFirst(isAggregate, o.isAggregate)
                .compare(name.toString(), o.name.toString())
                .compare(argumentTypes, o.argumentTypes, Ordering.<Type>natural().lexicographical())
                .compare(returnType, o.returnType)
                .result();
    }

    public static Function<FunctionInfo, QualifiedName> nameGetter()
    {
        return new Function<FunctionInfo, QualifiedName>()
        {
            @Override
            public QualifiedName apply(FunctionInfo input)
            {
                return input.getName();
            }
        };
    }

    public static Function<FunctionInfo, FunctionHandle> handleGetter()
    {
        return new Function<FunctionInfo, FunctionHandle>()
        {
            @Override
            public FunctionHandle apply(FunctionInfo input)
            {
                return input.getHandle();
            }
        };
    }

    public static Predicate<FunctionInfo> isAggregationPredicate()
    {
        return new Predicate<FunctionInfo>()
        {
            @Override
            public boolean apply(FunctionInfo functionInfo)
            {
                return functionInfo.isAggregate();
            }
        };
    }
}
