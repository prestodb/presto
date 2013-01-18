package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;

public class ExpressionUtils
{
    public static List<Expression> extractConjuncts(Expression expression)
    {
        if (expression instanceof LogicalBinaryExpression && ((LogicalBinaryExpression) expression).getType() == LogicalBinaryExpression.Type.AND) {
            LogicalBinaryExpression and = (LogicalBinaryExpression) expression;
            return ImmutableList.<Expression>builder()
                    .addAll(extractConjuncts(and.getLeft()))
                    .addAll(extractConjuncts(and.getRight()))
                    .build();
        }

        return ImmutableList.of(expression);
    }

}
