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

import com.facebook.presto.Session;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.NodeRef;

import java.util.Map;

import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static java.util.Collections.emptyList;

@Deprecated
public class AnalyzedExpressionRewriter
{
    private final Session session;
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final TypeProvider typeProvider;

    public AnalyzedExpressionRewriter(Session session, Metadata metadata, SqlParser sqlParser, TypeProvider typeProvider)
    {
        this.session = session;
        this.metadata = metadata;
        this.sqlParser = sqlParser;
        this.typeProvider = typeProvider;
    }

    public Expression rewriteWith(RewriterProvider<Void> rewriterProvider, Expression expression)
    {
        return rewriteWith(rewriterProvider, expression, null);
    }

    public <C> Expression rewriteWith(RewriterProvider<C> rewriterProvider, Expression expression, C context)
    {
        // Lambda cannot be analyzed outside the context of function, but its body can be rewritten.
        if (expression instanceof LambdaExpression) {
            LambdaExpression lambdaExpression = (LambdaExpression) expression;
            return new LambdaExpression(lambdaExpression.getArguments(), rewriteWith(rewriterProvider, lambdaExpression.getBody(), context));
        }
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(
                session,
                metadata,
                sqlParser,
                typeProvider,
                expression,
                emptyList(),
                WarningCollector.NOOP);
        return ExpressionTreeRewriter.rewriteWith(rewriterProvider.get(expressionTypes), expression, context);
    }

    interface RewriterProvider<C>
    {
        ExpressionRewriter<C> get(Map<NodeRef<Expression>, Type> expressionTypes);
    }
}
