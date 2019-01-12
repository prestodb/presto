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

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionRewriter;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.LambdaExpression;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.TryExpression;

public class DesugarTryExpressionRewriter
{
    private DesugarTryExpressionRewriter() {}

    public static Expression rewrite(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(), expression);
    }

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        @Override
        public Expression rewriteTryExpression(TryExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Expression expression = treeRewriter.rewrite(node.getInnerExpression(), context);

            return new FunctionCall(
                    QualifiedName.of("$internal$try"),
                    ImmutableList.of(new LambdaExpression(ImmutableList.of(), expression)));
        }
    }
}
