package com.facebook.presto.sql;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.tree.AliasedExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.base.Function;
import com.google.common.base.Predicate;

import javax.annotation.Nullable;
import java.util.Map;

public class AstFunctions
{
    public static Predicate<Expression> isAliasedAggregateFunction(final Metadata metadata)
    {
        return new Predicate<Expression>()
        {
            @Override
            public boolean apply(Expression input)
            {
                if (input instanceof AliasedExpression) {
                    return apply(((AliasedExpression) input).getExpression());
                }

                if (input instanceof FunctionCall) {
                    return metadata.getFunction(((FunctionCall) input).getName()).isAggregate();
                }

                return false;
            }
        };
    }

    public static Predicate<Expression> isAliasedQualifiedNameReference()
    {
        return new Predicate<Expression>()
        {
            @Override
            public boolean apply(Expression input)
            {
                if (input instanceof AliasedExpression) {
                    return apply(((AliasedExpression) input).getExpression());
                }

                return input instanceof QualifiedNameReference;
            }
        };
    }
}
