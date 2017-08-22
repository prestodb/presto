package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.tree.Expression;

public interface ExpressionRewriteRule
{
    Expression rewrite(Expression expression, Rule.Context context);
}
