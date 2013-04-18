package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.planner.Symbol;
import com.google.common.base.Preconditions;

import java.util.Map;

public class AnalyzedOutput
{
    private final TupleDescriptor descriptor;
    private final Map<Symbol, AnalyzedExpression> expressions;

    public AnalyzedOutput(TupleDescriptor descriptor, Map<Symbol, AnalyzedExpression> expressions)
    {
        Preconditions.checkNotNull(descriptor, "descriptor is null");
        Preconditions.checkNotNull(expressions, "expressions is null");

        this.descriptor = descriptor;
        this.expressions = expressions;
    }

    public Map<Symbol, AnalyzedExpression> getExpressions()
    {
        return expressions;
    }

    public TupleDescriptor getDescriptor()
    {
        return descriptor;
    }
}
