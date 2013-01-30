package com.facebook.presto.sql.analyzer;

import static com.google.common.base.Preconditions.checkNotNull;

public class AnalyzedJoinClause
{
    private final AnalyzedExpression left;
    private final AnalyzedExpression right;

    public AnalyzedJoinClause(AnalyzedExpression left, AnalyzedExpression right)
    {
        checkNotNull(left, "left is null");
        checkNotNull(right, "right is null");

        this.left = left;
        this.right = right;
    }

    public AnalyzedExpression getLeft()
    {
        return left;
    }

    public AnalyzedExpression getRight()
    {
        return right;
    }
}
