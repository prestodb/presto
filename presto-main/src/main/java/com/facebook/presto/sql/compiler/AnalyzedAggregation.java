package com.facebook.presto.sql.compiler;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AnalyzedAggregation that = (AnalyzedAggregation) o;

        if (!arguments.equals(that.arguments)) {
            return false;
        }
        if (!info.equals(that.info)) {
            return false;
        }
        if (!rewrittenCall.equals(that.rewrittenCall)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = info.hashCode();
        result = 31 * result + arguments.hashCode();
        result = 31 * result + rewrittenCall.hashCode();
        return result;
    }

    public static Function<AnalyzedAggregation, List<AnalyzedExpression>> argumentGetter()
    {
        return new Function<AnalyzedAggregation, List<AnalyzedExpression>>()
        {
            @Override
            public List<AnalyzedExpression> apply(AnalyzedAggregation input)
            {
                return input.getArguments();
            }
        };
    }
}
