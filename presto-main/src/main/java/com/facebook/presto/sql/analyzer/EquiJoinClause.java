package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;

public class EquiJoinClause
{
    private final Expression left;
    private final Expression right;

    public EquiJoinClause(Expression left, Expression right)
    {
        Preconditions.checkNotNull(left, "left is null");
        Preconditions.checkNotNull(right, "right is null");

        this.left = left;
        this.right = right;
    }

    public Expression getLeft()
    {
        return left;
    }

    public Expression getRight()
    {
        return right;
    }

    public static Function<EquiJoinClause, Expression> leftGetter()
    {
        return new Function<EquiJoinClause, Expression>()
        {
            @Override
            public Expression apply(EquiJoinClause input)
            {
                return input.getLeft();
            }
        };
    }

    public static Function<EquiJoinClause, Expression> rightGetter()
    {
        return new Function<EquiJoinClause, Expression>()
        {
            @Override
            public Expression apply(EquiJoinClause input)
            {
                return input.getRight();
            }
        };
    }
}
