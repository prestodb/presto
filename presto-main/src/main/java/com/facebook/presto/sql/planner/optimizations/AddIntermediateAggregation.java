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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.INTERMEDIATE;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static java.util.Objects.requireNonNull;

public class AddIntermediateAggregation
        extends PlanOptimizer
{
    private final Metadata metadata;

    public AddIntermediateAggregation(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");
        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        if (SystemSessionProperties.isIntermediateAggregation(session)) {
            return SimplePlanRewriter.rewriteWith(new Rewriter(idAllocator, metadata, symbolAllocator), plan);
        }
        return plan;
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final SymbolAllocator symbolAllocator;

        private Rewriter(PlanNodeIdAllocator idAllocator, Metadata metadata, SymbolAllocator symbolAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            if (node.getStep() != PARTIAL || node.getGroupBy().isEmpty()) {
                return context.defaultRewrite(node);
            }
            PlanNode rewrittenSource = context.defaultRewrite(node.getSource());
            Map<Symbol, Symbol> masks = node.getMasks();

            Map<Symbol, FunctionCall> finalCalls = new HashMap<>();
            Map<Symbol, FunctionCall> initialCalls = new HashMap<>();
            Map<Symbol, Signature> initialFunctions = new HashMap<>();
            Map<Symbol, Symbol> initialMask = new HashMap<>();

            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Signature signature = node.getFunctions().get(entry.getKey());
                InternalAggregationFunction function = metadata.getFunctionRegistry().getAggregateFunctionImplementation(signature);

                Symbol initialSymbol = symbolAllocator.newSymbol(signature.getName(), function.getIntermediateType());
                initialCalls.put(initialSymbol, entry.getValue());
                initialFunctions.put(initialSymbol, signature);
                if (masks.containsKey(entry.getKey())) {
                    initialMask.put(initialSymbol, masks.get(entry.getKey()));
                }

                // rewrite final aggregation in terms of intermediate function
                finalCalls.put(entry.getKey(),
                        new FunctionCall(QualifiedName.of(signature.getName()), ImmutableList.<Expression>of(new QualifiedNameReference(initialSymbol.toQualifiedName()))));
            }

            PlanNode partialAggregation = new AggregationNode(
                    idAllocator.getNextId(),
                    rewrittenSource,
                    node.getGroupBy(),
                    initialCalls,
                    initialFunctions,
                    initialMask,
                    node.getGroupingSets(),
                    PARTIAL,
                    node.getSampleWeight(),
                    node.getConfidence(),
                    node.getHashSymbol());

            return new AggregationNode(
                    idAllocator.getNextId(),
                    partialAggregation,
                    node.getGroupBy(),
                    finalCalls,
                    node.getFunctions(),
                    ImmutableMap.of(),
                    node.getGroupingSets(),
                    INTERMEDIATE,
                    Optional.empty(),
                    node.getConfidence(),
                    node.getHashSymbol());
        }
    }
}
