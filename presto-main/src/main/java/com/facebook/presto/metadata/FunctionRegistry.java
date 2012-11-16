package com.facebook.presto.metadata;

import com.facebook.presto.operator.aggregation.CountAggregation;
import com.facebook.presto.operator.aggregation.DoubleAverageAggregation;
import com.facebook.presto.operator.aggregation.DoubleSumAggregation;
import com.facebook.presto.operator.aggregation.LongAverageAggregation;
import com.facebook.presto.operator.aggregation.LongSumAggregation;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import java.util.List;

import static com.facebook.presto.tuple.TupleInfo.Type.*;
import static java.lang.String.format;

public class FunctionRegistry
{
    private final Multimap<QualifiedName, FunctionInfo> functions;

    public FunctionRegistry()
    {
        functions = buildFunctions(
                new FunctionInfo(QualifiedName.of("count"), FIXED_INT_64, ImmutableList.<TupleInfo.Type>of(), FIXED_INT_64, CountAggregation.BINDER),
                new FunctionInfo(QualifiedName.of("sum"), FIXED_INT_64, ImmutableList.of(FIXED_INT_64), FIXED_INT_64, LongSumAggregation.BINDER),
                new FunctionInfo(QualifiedName.of("sum"), DOUBLE, ImmutableList.of(DOUBLE), DOUBLE, DoubleSumAggregation.BINDER),
                new FunctionInfo(QualifiedName.of("avg"), DOUBLE, ImmutableList.of(DOUBLE), VARIABLE_BINARY, DoubleAverageAggregation.BINDER),
                new FunctionInfo(QualifiedName.of("avg"), DOUBLE, ImmutableList.of(FIXED_INT_64), VARIABLE_BINARY, LongAverageAggregation.BINDER)
        );
    }

    public FunctionInfo get(QualifiedName name, List<TupleInfo.Type> parameterTypes)
    {
        for (FunctionInfo functionInfo : functions.get(name)) {
            if (functionInfo.getArgumentTypes().equals(parameterTypes)) {
                return functionInfo;
            }
        }

        throw new IllegalArgumentException(format("Function %s(%s) not registered", name, Joiner.on(", ").join(parameterTypes)));
    }

    private Multimap<QualifiedName, FunctionInfo> buildFunctions(FunctionInfo... infos)
    {
        return Multimaps.index(ImmutableList.copyOf(infos), new Function<FunctionInfo, QualifiedName>()
        {
            @Override
            public QualifiedName apply(FunctionInfo input)
            {
                return input.getName();
            }
        });
    }


}
