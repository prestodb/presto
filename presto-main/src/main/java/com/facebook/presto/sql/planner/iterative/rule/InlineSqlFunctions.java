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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.SqlInvokedScalarFunctionImplementation;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isInlineSqlFunctions;
import static com.facebook.presto.sql.relational.SqlFunctionUtils.getSqlFunctionRowExpression;
import static java.util.Objects.requireNonNull;

public class InlineSqlFunctions
        extends RowExpressionRewriteRuleSet
{
    public InlineSqlFunctions(Metadata metadata, SqlParser sqlParser)
    {
        super(createRewrite(metadata, sqlParser));
    }

    private static PlanRowExpressionRewriter createRewrite(Metadata metadata, SqlParser sqlParser)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(sqlParser, "sqlParser is null");

        return (expression, context) -> InlineSqlFunctionsRewriter.rewrite(
                expression,
                context.getSession(),
                metadata);
    }

    @Override
    public Set<Rule<?>> rules()
    {
        // Aggregations are not rewritten because they cannot have SQL functions
        return ImmutableSet.of(
                projectRowExpressionRewriteRule(),
                filterRowExpressionRewriteRule(),
                joinRowExpressionRewriteRule(),
                valueRowExpressionRewriteRule());
    }

    public static class InlineSqlFunctionsRewriter
    {
        private InlineSqlFunctionsRewriter() {}

        public static RowExpression rewrite(RowExpression expression, Session session, Metadata metadata)
        {
            if (isInlineSqlFunctions(session)) {
                return RowExpressionTreeRewriter.rewriteWith(new Visitor(session, metadata), expression);
            }
            return expression;
        }

        private static class Visitor
                extends RowExpressionRewriter<Void>
        {
            private final Session session;
            private final Metadata metadata;

            public Visitor(Session session, Metadata metadata)
            {
                this.session = requireNonNull(session, "session is null");
                this.metadata = requireNonNull(metadata, "metadata is null");
            }

            @Override
            public RowExpression rewriteCall(CallExpression expression, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
            {
                List<RowExpression> rewrittenArguments = new ArrayList<>();
                for (RowExpression argument : expression.getArguments()) {
                    rewrittenArguments.add(treeRewriter.rewrite(argument, context));
                }

                FunctionHandle functionHandle = expression.getFunctionHandle();
                FunctionMetadata functionMetadata = metadata.getFunctionAndTypeManager().getFunctionMetadata(functionHandle);

                if (functionMetadata.getImplementationType() != FunctionImplementationType.SQL) {
                    return new CallExpression(expression.getSourceLocation(), expression.getDisplayName(), functionHandle, expression.getType(), rewrittenArguments);
                }
                return getSqlFunctionRowExpression(
                        functionMetadata,
                        (SqlInvokedScalarFunctionImplementation) metadata.getFunctionAndTypeManager().getScalarFunctionImplementation(functionHandle),
                        metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver(),
                        session.getSqlFunctionProperties(),
                        session.getSessionFunctions(),
                        rewrittenArguments);
            }
        }
    }
}
