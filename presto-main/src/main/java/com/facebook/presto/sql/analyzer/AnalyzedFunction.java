package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class AnalyzedFunction
{
    private final FunctionInfo info;
    private final List<AnalyzedExpression> arguments;
    private final FunctionCall rewrittenCall;
    private final boolean distinct;
    private final Optional<AnalyzedWindow> window;

    public AnalyzedFunction(FunctionInfo info, List<AnalyzedExpression> arguments, FunctionCall rewrittenCall, boolean distinct, AnalyzedWindow window)
    {
        Preconditions.checkNotNull(info, "info is null");
        Preconditions.checkNotNull(arguments, "arguments is null");
        Preconditions.checkNotNull(rewrittenCall, "rewrittenCall is null");

        this.info = info;
        this.arguments = ImmutableList.copyOf(arguments);
        this.rewrittenCall = rewrittenCall;
        this.distinct = distinct;
        this.window = Optional.fromNullable(window);
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

    public boolean isDistinct()
    {
        return distinct;
    }

    public Optional<AnalyzedWindow> getWindow()
    {
        return window;
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
        AnalyzedFunction other = (AnalyzedFunction) obj;
        return Objects.equal(info, other.info) &&
                Objects.equal(arguments, other.arguments) &&
                Objects.equal(rewrittenCall, other.rewrittenCall) &&
                Objects.equal(distinct, other.distinct) &&
                Objects.equal(window, other.window);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(info, arguments, rewrittenCall, distinct, window);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("info", info)
                .add("arguments", arguments)
                .add("rewrittenCall", rewrittenCall)
                .add("distinct", distinct)
                .add("window", window)
                .toString();
    }

    public static Function<AnalyzedFunction, List<AnalyzedExpression>> argumentGetter()
    {
        return new Function<AnalyzedFunction, List<AnalyzedExpression>>()
        {
            @Override
            public List<AnalyzedExpression> apply(AnalyzedFunction input)
            {
                return input.getArguments();
            }
        };
    }

    public static Predicate<AnalyzedFunction> distinctPredicate()
    {
        return new Predicate<AnalyzedFunction>()
        {
            @Override
            public boolean apply(AnalyzedFunction input)
            {
                return input.isDistinct();
            }
        };
    }

    public static Function<AnalyzedFunction, List<AnalyzedExpression>> windowExpressionGetter()
    {
        return new Function<AnalyzedFunction, List<AnalyzedExpression>>()
        {
            @Override
            public List<AnalyzedExpression> apply(AnalyzedFunction input)
            {
                checkState(input.getWindow().isPresent(), "not a window function");
                return AnalyzedWindow.expressionGetter().apply(input.getWindow().get());
            }
        };
    }
}
