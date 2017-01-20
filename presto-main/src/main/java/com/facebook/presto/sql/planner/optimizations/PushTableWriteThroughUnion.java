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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;

import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class PushTableWriteThroughUnion
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

        if (!SystemSessionProperties.isPushTableWriteThroughUnion(session)) {
            return plan;
        }

        return SimplePlanRewriter.rewriteWith(new Rewriter(idAllocator, symbolAllocator), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;

        public Rewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, RewriteContext<Void> context)
        {
            PlanNode sourceNode = context.rewrite(node.getSource());

            if (node.getPartitioningScheme().isPresent()) {
                // The primary incentive of this optimizer is to increase the parallelism for table
                // write. For a table with partitioning scheme, parallelism for table writing is
                // guaranteed regardless of this optimizer. The level of local parallelism will be
                // determined by LocalExecutionPlanner separately, and shouldn't be a concern of
                // this optimizer.
                return node;
            }

            // if sourceNode is not a UNION ALL, don't perform this optimization. A UNION DISTINCT would have an aggregationNode as source
            if (!(sourceNode instanceof UnionNode)) {
                return node;
            }

            UnionNode unionNode = (UnionNode) sourceNode;
            ImmutableList.Builder<PlanNode> rewrittenSources = ImmutableList.builder();
            ImmutableListMultimap.Builder<Symbol, Symbol> mappings = ImmutableListMultimap.builder();
            for (int i = 0; i < unionNode.getSources().size(); i++) {
                PlanNode unionOriginalSource = unionNode.getSources().get(i);
                ImmutableList.Builder<Symbol> newSymbols = ImmutableList.builder();
                for (Symbol outputSymbol : node.getOutputSymbols()) {
                    Symbol newSymbol = symbolAllocator.newSymbol(outputSymbol.getName(), symbolAllocator.getTypes().get(outputSymbol));
                    newSymbols.add(newSymbol);
                    mappings.put(outputSymbol, newSymbol);
                }

                int index = i;
                rewrittenSources.add(new TableWriterNode(
                        idAllocator.getNextId(),
                        unionOriginalSource,
                        node.getTarget(),
                        node.getColumns().stream()
                            .map(column -> unionNode.getSymbolMapping().get(column).get(index))
                            .collect(Collectors.toList()),
                        node.getColumnNames(),
                        newSymbols.build(),
                        node.getPartitioningScheme()));
            }

            return new UnionNode(idAllocator.getNextId(), rewrittenSources.build(), mappings.build(), ImmutableList.copyOf(mappings.build().keySet()));
        }
    }
}
