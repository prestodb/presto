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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.SqlInvokedScalarFunctionImplementation;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isInlineSqlFunctions;
import static com.facebook.presto.metadata.FunctionAndTypeManager.qualifyObjectName;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.SqlFunctionUtils.getSqlFunctionExpression;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class InlineSqlFunctions
        extends ExpressionRewriteRuleSet
{
    public InlineSqlFunctions(Metadata metadata, SqlParser sqlParser)
    {
        super(createRewrite(metadata, sqlParser));
    }

    private static ExpressionRewriter createRewrite(Metadata metadata, SqlParser sqlParser)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(sqlParser, "sqlParser is null");

        return (expression, context) -> InlineSqlFunctionsRewriter.rewrite(
                expression,
                context.getSession(),
                metadata,
                context.getVariableAllocator(),
                getExpressionTypes(
                        context.getSession(),
                        metadata,
                        sqlParser,
                        context.getVariableAllocator().getTypes(),
                        expression,
                        emptyList(),
                        context.getWarningCollector()));
    }

    @Override
    public Set<Rule<?>> rules()
    {
        // Aggregations are not rewritten because they cannot have SQL functions
        return ImmutableSet.of(
                projectExpressionRewrite(),
                filterExpressionRewrite(),
                joinExpressionRewrite(),
                valuesExpressionRewrite());
    }

    public static class InlineSqlFunctionsRewriter
    {
        private InlineSqlFunctionsRewriter() {}

        public static Expression rewrite(Expression expression, Session session, Metadata metadata, PlanVariableAllocator variableAllocator, Map<NodeRef<Expression>, Type> expressionTypes)
        {
            if (isInlineSqlFunctions(session)) {
                return ExpressionTreeRewriter.rewriteWith(new Visitor(session, metadata, variableAllocator, expressionTypes), expression);
            }
            return expression;
        }

        private static class Visitor
                extends com.facebook.presto.sql.tree.ExpressionRewriter<Void>
        {
            private final Session session;
            private final Metadata metadata;
            private final PlanVariableAllocator variableAllocator;
            private final Map<NodeRef<Expression>, Type> expressionTypes;

            public Visitor(Session session, Metadata metadata, PlanVariableAllocator variableAllocator, Map<NodeRef<Expression>, Type> expressionTypes)
            {
                this.session = requireNonNull(session, "session is null");
                this.metadata = requireNonNull(metadata, "metadata is null");
                this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
                this.expressionTypes = expressionTypes;
            }

            @Override
            public Expression rewriteFunctionCall(FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                List<Type> argumentTypes = new ArrayList<>();
                List<Expression> rewrittenArguments = new ArrayList<>();
                for (Expression argument : node.getArguments()) {
                    argumentTypes.add(expressionTypes.get(NodeRef.of(argument)));
                    rewrittenArguments.add(treeRewriter.rewrite(argument, context));
                }

                FunctionHandle functionHandle = metadata.getFunctionAndTypeManager().resolveFunction(
                        Optional.of(session.getSessionFunctions()),
                        session.getTransactionId(),
                        qualifyObjectName(node.getName()),
                        fromTypes(argumentTypes));
                FunctionMetadata functionMetadata = metadata.getFunctionAndTypeManager().getFunctionMetadata(functionHandle);

                if (functionMetadata.getImplementationType() != FunctionImplementationType.SQL) {
                    return new FunctionCall(node.getName(), rewrittenArguments);
                }
                return getSqlFunctionExpression(
                        functionMetadata,
                        (SqlInvokedScalarFunctionImplementation) metadata.getFunctionAndTypeManager().getScalarFunctionImplementation(functionHandle),
                        metadata,
                        variableAllocator,
                        session.getSqlFunctionProperties(),
                        rewrittenArguments);
            }
        }
    }
}
