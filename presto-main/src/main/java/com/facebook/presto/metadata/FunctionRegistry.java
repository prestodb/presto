package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import java.util.List;

import static java.lang.String.format;

public class FunctionRegistry
{
    private final Multimap<QualifiedName, FunctionInfo> functions;

    public FunctionRegistry()
    {
        functions = buildFunctions(
                new FunctionInfo(QualifiedName.of("count"), true, TupleInfo.Type.FIXED_INT_64, ImmutableList.<TupleInfo.Type>of()),
                new FunctionInfo(QualifiedName.of("sum"), true, TupleInfo.Type.FIXED_INT_64, ImmutableList.of(TupleInfo.Type.FIXED_INT_64)),
                new FunctionInfo(QualifiedName.of("sum"), true, TupleInfo.Type.DOUBLE, ImmutableList.of(TupleInfo.Type.DOUBLE)),
                new FunctionInfo(QualifiedName.of("avg"), true, TupleInfo.Type.DOUBLE, ImmutableList.of(TupleInfo.Type.DOUBLE)),
                new FunctionInfo(QualifiedName.of("avg"), true, TupleInfo.Type.DOUBLE, ImmutableList.of(TupleInfo.Type.FIXED_INT_64))
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
