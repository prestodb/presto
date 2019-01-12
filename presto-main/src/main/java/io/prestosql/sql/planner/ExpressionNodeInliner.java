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
package io.prestosql.sql.planner;

import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionRewriter;
import io.prestosql.sql.tree.ExpressionTreeRewriter;

import java.util.Map;

public class ExpressionNodeInliner
        extends ExpressionRewriter<Void>
{
    public static Expression replaceExpression(Expression expression, Map<? extends Expression, ? extends Expression> mappings)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionNodeInliner(mappings), expression);
    }

    private final Map<? extends Expression, ? extends Expression> mappings;

    public ExpressionNodeInliner(Map<? extends Expression, ? extends Expression> mappings)
    {
        this.mappings = mappings;
    }

    @Override
    public Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        return mappings.get(node);
    }
}
