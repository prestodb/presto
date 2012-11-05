package com.facebook.presto.sql.compiler;

import java.util.Map;

public class AnalyzedOutput
{
    private final TupleDescriptor descriptor;
    private final Map<Slot, AnalyzedExpression> expressions;

    public AnalyzedOutput(TupleDescriptor descriptor, Map<Slot, AnalyzedExpression> expressions)
    {
        this.descriptor = descriptor;
        this.expressions = expressions;
    }

    public Map<Slot, AnalyzedExpression> getExpressions()
    {
        return expressions;
    }

    public TupleDescriptor getDescriptor()
    {
        return descriptor;
    }
}
