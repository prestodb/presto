package com.facebook.presto.metadata;

import com.facebook.presto.operator.AggregationFunctionDefinition;
import com.facebook.presto.operator.aggregation.Input;
import com.facebook.presto.operator.aggregation.NewAggregationFunction;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import java.util.List;

import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;

public class FunctionInfo
{
    private final int id;

    private final QualifiedName name;
    private final TupleInfo.Type returnType;
    private final List<TupleInfo.Type> argumentTypes;

    private final boolean isAggregate;
    private final TupleInfo.Type intermediateType;
    private final NewAggregationFunction function;

    public FunctionInfo(int id, QualifiedName name, TupleInfo.Type returnType, List<TupleInfo.Type> argumentTypes, TupleInfo.Type intermediateType, NewAggregationFunction function)
    {
        this.id = id;
        this.name = name;
        this.returnType = returnType;
        this.argumentTypes = argumentTypes;
        this.intermediateType = intermediateType;
        this.function = function;
        this.isAggregate = true;
    }

    public FunctionInfo(int id, QualifiedName name, TupleInfo.Type returnType, List<TupleInfo.Type> argumentTypes)
    {
        this.id = id;
        this.name = name;
        this.returnType = returnType;
        this.argumentTypes = argumentTypes;

        this.isAggregate = false;
        this.intermediateType = null;
        this.function = null;
    }

    public FunctionHandle getHandle()
    {
        return new FunctionHandle(id, name.toString());
    }

    public QualifiedName getName()
    {
        return name;
    }

    public boolean isAggregate()
    {
        return isAggregate;
    }

    public TupleInfo.Type getReturnType()
    {
        return returnType;
    }

    public List<TupleInfo.Type> getArgumentTypes()
    {
        return argumentTypes;
    }

    public TupleInfo.Type getIntermediateType()
    {
        return intermediateType;
    }

    public AggregationFunctionDefinition bind(List<Input> inputs)
    {
        if (inputs.isEmpty()) {
            return aggregation(function, -1);
        } else {
            Preconditions.checkArgument(inputs.size() == 1, "expected at most one input");
            Input input = inputs.get(0);
            Preconditions.checkArgument(input.getField() == 0, "expected field to be 0");
            return aggregation(function, input.getChannel());
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FunctionInfo that = (FunctionInfo) o;

        if (isAggregate != that.isAggregate) {
            return false;
        }
        if (!argumentTypes.equals(that.argumentTypes)) {
            return false;
        }
        if (intermediateType != that.intermediateType) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        if (returnType != that.returnType) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = name.hashCode();
        result = 31 * result + (isAggregate ? 1 : 0);
        result = 31 * result + returnType.hashCode();
        result = 31 * result + argumentTypes.hashCode();
        result = 31 * result + (intermediateType != null ? intermediateType.hashCode() : 0);
        return result;
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
}
