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
package com.facebook.presto.sql.planner.sanity;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ListMultimap;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

/**
 * Ensures that all the expressions and FunctionCalls matches their output symbols
 */
public final class TypeValidator
        implements PlanSanityChecker.Checker
{
    public TypeValidator() {}

    @Override
    public void validate(PlanNode plan, Session session, Metadata metadata, SqlParser sqlParser, TypeProvider types)
    {
        plan.accept(new Visitor(session, metadata, sqlParser, types), null);
    }

    private static class Visitor
            extends SimplePlanVisitor<Void>
    {
        private final Session session;
        private final Metadata metadata;
        private final SqlParser sqlParser;
        private final TypeProvider types;

        public Visitor(Session session, Metadata metadata, SqlParser sqlParser, TypeProvider types)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.types = requireNonNull(types, "types is null");
        }

        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            visitPlan(node, context);

            AggregationNode.Step step = node.getStep();

            switch (step) {
                case SINGLE:
                    checkFunctionSignature(node.getAggregations());
                    checkFunctionCall(node.getAggregations());
                    break;
                case FINAL:
                    checkFunctionSignature(node.getAggregations());
                    break;
            }

            return null;
        }

        @Override
        public Void visitWindow(WindowNode node, Void context)
        {
            visitPlan(node, context);

            checkWindowFunctions(node.getWindowFunctions());

            return null;
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            visitPlan(node, context);

            for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
                Type expectedType = types.get(entry.getKey());
                if (entry.getValue() instanceof SymbolReference) {
                    SymbolReference symbolReference = (SymbolReference) entry.getValue();
                    verifyTypeSignature(entry.getKey(), expectedType.getTypeSignature(), types.get(Symbol.from(symbolReference)).getTypeSignature());
                    continue;
                }
                Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(session, metadata, sqlParser, types, entry.getValue(), emptyList() /* parameters already replaced */);
                Type actualType = expressionTypes.get(NodeRef.of(entry.getValue()));
                verifyTypeSignature(entry.getKey(), expectedType.getTypeSignature(), actualType.getTypeSignature());
            }

            return null;
        }

        @Override
        public Void visitUnion(UnionNode node, Void context)
        {
            visitPlan(node, context);

            ListMultimap<Symbol, Symbol> symbolMapping = node.getSymbolMapping();
            for (Symbol keySymbol : symbolMapping.keySet()) {
                List<Symbol> valueSymbols = symbolMapping.get(keySymbol);
                Type expectedType = types.get(keySymbol);
                for (Symbol valueSymbol : valueSymbols) {
                    verifyTypeSignature(keySymbol, expectedType.getTypeSignature(), types.get(valueSymbol).getTypeSignature());
                }
            }

            return null;
        }

        private void checkWindowFunctions(Map<Symbol, WindowNode.Function> functions)
        {
            for (Map.Entry<Symbol, WindowNode.Function> entry : functions.entrySet()) {
                Signature signature = entry.getValue().getSignature();
                FunctionCall call = entry.getValue().getFunctionCall();

                checkSignature(entry.getKey(), signature);
                checkCall(entry.getKey(), call);
            }
        }

        private void checkSignature(Symbol symbol, Signature signature)
        {
            TypeSignature expectedTypeSignature = types.get(symbol).getTypeSignature();
            TypeSignature actualTypeSignature = signature.getReturnType();
            verifyTypeSignature(symbol, expectedTypeSignature, actualTypeSignature);
        }

        private void checkCall(Symbol symbol, FunctionCall call)
        {
            Type expectedType = types.get(symbol);
            Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(session, metadata, sqlParser, types, call, emptyList() /*parameters already replaced */);
            Type actualType = expressionTypes.get(NodeRef.<Expression>of(call));
            verifyTypeSignature(symbol, expectedType.getTypeSignature(), actualType.getTypeSignature());
        }

        private void checkFunctionSignature(Map<Symbol, Aggregation> aggregations)
        {
            for (Map.Entry<Symbol, Aggregation> entry : aggregations.entrySet()) {
                checkSignature(entry.getKey(), entry.getValue().getSignature());
            }
        }

        private void checkFunctionCall(Map<Symbol, Aggregation> aggregations)
        {
            for (Map.Entry<Symbol, Aggregation> entry : aggregations.entrySet()) {
                checkCall(entry.getKey(), entry.getValue().getCall());
            }
        }

        private void verifyTypeSignature(Symbol symbol, TypeSignature expected, TypeSignature actual)
        {
            // UNKNOWN should be considered as a wildcard type, which matches all the other types
            TypeManager typeManager = metadata.getTypeManager();
            if (!actual.equals(UNKNOWN.getTypeSignature()) && !typeManager.isTypeOnlyCoercion(typeManager.getType(actual), typeManager.getType(expected))) {
                checkArgument(expected.equals(actual), "type of symbol '%s' is expected to be %s, but the actual type is %s", symbol, expected, actual);
            }
        }
    }
}
