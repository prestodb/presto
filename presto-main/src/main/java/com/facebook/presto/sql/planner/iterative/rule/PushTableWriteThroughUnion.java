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

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isPushTableWriteThroughUnion;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class PushTableWriteThroughUnion
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Context context)
    {
        if (!isPushTableWriteThroughUnion(context.getSession())) {
            return Optional.empty();
        }

        if (!(node instanceof TableWriterNode)) {
            return Optional.empty();
        }

        TableWriterNode tableWriterNode = (TableWriterNode) node;
        if (tableWriterNode.getPartitioningScheme().isPresent()) {
            // The primary incentive of this optimizer is to increase the parallelism for table
            // write. For a table with partitioning scheme, parallelism for table writing is
            // guaranteed regardless of this optimizer. The level of local parallelism will be
            // determined by LocalExecutionPlanner separately, and shouldn't be a concern of
            // this optimizer.
            return Optional.empty();
        }

        PlanNode child = context.getLookup().resolve(tableWriterNode.getSource());
        if (!(child instanceof UnionNode)) {
            return Optional.empty();
        }

        UnionNode unionNode = (UnionNode) child;
        ImmutableList.Builder<PlanNode> rewrittenSources = ImmutableList.builder();
        ImmutableListMultimap.Builder<Symbol, Symbol> mappings = ImmutableListMultimap.builder();
        for (int i = 0; i < unionNode.getSources().size(); i++) {
            int index = i;
            ImmutableList.Builder<Symbol> newSymbols = ImmutableList.builder();
            for (Symbol outputSymbol : node.getOutputSymbols()) {
                Symbol newSymbol = context.getSymbolAllocator().newSymbol(outputSymbol);
                newSymbols.add(newSymbol);
                mappings.put(outputSymbol, newSymbol);
            }
            rewrittenSources.add(new TableWriterNode(
                    context.getIdAllocator().getNextId(),
                    unionNode.getSources().get(index),
                    tableWriterNode.getTarget(),
                    tableWriterNode.getColumns().stream()
                            .map(column -> unionNode.getSymbolMapping().get(column).get(index))
                            .collect(toImmutableList()),
                    tableWriterNode.getColumnNames(),
                    newSymbols.build(),
                    tableWriterNode.getPartitioningScheme()));
        }

        return Optional.of(new UnionNode(context.getIdAllocator().getNextId(), rewrittenSources.build(), mappings.build(), ImmutableList.copyOf(mappings.build().keySet())));
    }
}
