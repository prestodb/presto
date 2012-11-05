package com.facebook.presto.metadata;

import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Objects;
import org.jetbrains.annotations.Nullable;

import javax.inject.Provider;
import java.util.List;
import java.util.Map;

public class FunctionInfo
{
    private final QualifiedName name;
    private final boolean isAggregate;
    private final TupleInfo.Type returnType;
    private final List<TupleInfo.Type> argumentTypes;

    private final Map<TupleInfo.Type, Provider<AggregationFunction>> providers;
    private final Provider<AggregationFunction> defaultProvider;

    public FunctionInfo(QualifiedName name, boolean aggregate, TupleInfo.Type returnType, List<TupleInfo.Type> argumentTypes, Map<TupleInfo.Type, Provider<AggregationFunction>> providers, @Nullable Provider<AggregationFunction> defaultProvider)
    {
        this.name = name;
        isAggregate = aggregate;
        this.returnType = returnType;
        this.argumentTypes = argumentTypes;
        this.providers = providers;
        this.defaultProvider = defaultProvider;
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

    public Provider<AggregationFunction> getProvider(TupleInfo.Type paramType)
    {
        return Objects.firstNonNull(providers.get(paramType), defaultProvider);
    }
}
