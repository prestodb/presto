/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.StringLiteral;

import static com.facebook.presto.sql.tree.ComparisonExpression.Type.EQUAL;

public final class InterpretedTestHelper {
    public InterpretedTestHelper()
    {
    }

    public static final NullLiteral NULL_LITERAL = new NullLiteral();

    public static Expression booleanLiteral(boolean value)
    {
        Expression leftExpression;
        if (value) {
            leftExpression = new ComparisonExpression(EQUAL, new LongLiteral("1"), new LongLiteral("1"));
        } else {
            leftExpression = new ComparisonExpression(EQUAL, new LongLiteral("1"), new LongLiteral("2"));
        }
        return leftExpression;
    }

    public static Expression longLiteral(long value)
    {
        return new LongLiteral(String.valueOf(value));
    }

    public static Expression doubleLiteral(double value)
    {
        return new DoubleLiteral(String.valueOf(value));
    }

    public static Expression stringLiteral(String value)
    {
        return new StringLiteral(value);
    }
}
