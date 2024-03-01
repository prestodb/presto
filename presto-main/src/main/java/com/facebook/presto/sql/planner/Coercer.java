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
package com.facebook.presto.sql.planner;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.NodeRef;

import java.util.Map;
import java.util.Set;

public class Coercer
{
    private Coercer()
    {
    }

    public static Expression addCoercions(Expression expression, Analysis analysis)
    {
        return ExpressionTreeRewriter.rewriteWith(new Rewriter(analysis.getCoercions(), analysis.getTypeOnlyCoercions()), expression);
    }

    public static Expression addCoercions(Expression expression, Map<NodeRef<Expression>, Type> coercions, Set<NodeRef<Expression>> typeOnlyCoercions)
    {
        return ExpressionTreeRewriter.rewriteWith(new Rewriter(coercions, typeOnlyCoercions), expression);
    }

    private static class Rewriter
            extends ExpressionRewriter<Void>
    {
        private final Map<NodeRef<Expression>, Type> coercions;
        private final Set<NodeRef<Expression>> typeOnlyCoercions;

        public Rewriter(Map<NodeRef<Expression>, Type> coercions, Set<NodeRef<Expression>> typeOnlyCoercions)
        {
            this.coercions = coercions;
            this.typeOnlyCoercions = typeOnlyCoercions;
        }

        @Override
        public Expression rewriteExpression(Expression expression, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Type target = coercions.get(NodeRef.of(expression));

            Expression rewritten = treeRewriter.defaultRewrite(expression, null);
            if (target != null) {
                rewritten = new Cast(
                        rewritten,
                        target.getTypeSignature().toString(),
                        false,
                        typeOnlyCoercions.contains(NodeRef.of(expression)));
            }

            return rewritten;
        }
    }
}
