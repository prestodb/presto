package com.facebook.presto.sql.compiler;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class AnalyzedAggregation
{
    private final FunctionInfo info;
    private final List<AnalyzedExpression> arguments;
    private final FunctionCall rewrittenCall;

    public AnalyzedAggregation(FunctionInfo info, List<AnalyzedExpression> arguments, FunctionCall rewrittenCall)
    {
        Preconditions.checkNotNull(info, "info is null");
        Preconditions.checkNotNull(arguments, "arguments is null");
        Preconditions.checkNotNull(rewrittenCall, "rewrittenCall is null");

        this.info = info;
        this.arguments = ImmutableList.copyOf(arguments);
        this.rewrittenCall = rewrittenCall;
    }

    public QualifiedName getFunctionName()
    {
        return info.getName();
    }

    public List<AnalyzedExpression> getArguments()
    {
        return arguments;
    }

    public FunctionInfo getFunctionInfo()
    {
        return info;
    }

    public Type getType()
    {
        return Type.fromRaw(info.getReturnType());
    }

    public FunctionCall getRewrittenCall()
    {
        return rewrittenCall;
    }
}
