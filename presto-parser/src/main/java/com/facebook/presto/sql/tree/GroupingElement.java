/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.tree;

import java.util.List;
import java.util.Optional;

public abstract class GroupingElement
        extends Node
{
    protected GroupingElement(Optional<NodeLocation> location)
    {
        super(location);
    }

    public abstract List<Expression> getExpressions();

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitGroupingElement(this, context);
    }

    void validateExpressions(List<Expression> expressions)
    {
        expressions.forEach(expression -> ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteLambdaExpression(LambdaExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                throw new UnsupportedOperationException("GROUP BY does not support lambda expressions, please use GROUP BY # instead");
            }
        }, expression, null));
    }
}
