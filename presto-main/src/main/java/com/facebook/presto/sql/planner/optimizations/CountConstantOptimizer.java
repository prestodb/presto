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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static java.util.Objects.requireNonNull;

public class CountConstantOptimizer
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            Map<Symbol, FunctionCall> aggregations = new LinkedHashMap<>(node.getAggregations());
            Map<Symbol, Signature> functions = new LinkedHashMap<>(node.getFunctions());

            PlanNode source = context.rewrite(node.getSource());
            if (source instanceof ProjectNode) {
                ProjectNode projectNode = (ProjectNode) source;
                for (Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                    Symbol symbol = entry.getKey();
                    FunctionCall functionCall = entry.getValue();
                    Signature signature = node.getFunctions().get(symbol);
                    if (isCountConstant(projectNode, functionCall, signature)) {
                        aggregations.put(symbol, new FunctionCall(functionCall.getName(), functionCall.getWindow(), functionCall.getFilter(), functionCall.isDistinct(), ImmutableList.of()));
                        functions.put(symbol, new Signature("count", AGGREGATE, parseTypeSignature(StandardTypes.BIGINT)));
                    }
                }
            }

            return new AggregationNode(
                    node.getId(),
                    source,
                    aggregations,
                    functions,
                    node.getMasks(),
                    node.getGroupingSets(),
                    node.getStep(),
                    node.getHashSymbol(),
                    node.getGroupIdSymbol());
        }

        public static boolean isCountConstant(ProjectNode projectNode, FunctionCall functionCall, Signature signature)
        {
            if (!"count".equals(signature.getName()) ||
                    signature.getArgumentTypes().size() != 1 ||
                    !signature.getReturnType().getBase().equals(StandardTypes.BIGINT)) {
                return false;
            }

            Expression argument = functionCall.getArguments().get(0);
            if (argument instanceof Literal && !(argument instanceof NullLiteral)) {
                return true;
            }

            if (argument instanceof SymbolReference) {
                Symbol argumentSymbol = Symbol.from(argument);
                Expression argumentExpression = projectNode.getAssignments().get(argumentSymbol);
                return (argumentExpression instanceof Literal) && (!(argumentExpression instanceof NullLiteral));
            }

            return false;
        }
    }
}
