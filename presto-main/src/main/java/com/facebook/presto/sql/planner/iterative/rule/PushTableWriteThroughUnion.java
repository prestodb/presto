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
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.SymbolMapper;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.isPushTableWriteThroughUnion;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.optimizations.SymbolMapper.createSymbolMapper;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.tableWriterNode;
import static com.facebook.presto.sql.planner.plan.Patterns.union;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class PushTableWriteThroughUnion
        implements Rule<TableWriterNode>
{
    private static final Capture<UnionNode> CHILD = newCapture();

    private static final Pattern<TableWriterNode> PATTERN = tableWriterNode()
            // The primary incentive of this optimizer is to increase the parallelism for table
            // write. For a table with partitioning scheme, parallelism for table writing is
            // guaranteed regardless of this optimizer. The level of local parallelism will be
            // determined by LocalExecutionPlanner separately, and shouldn't be a concern of
            // this optimizer.
            .matching(tableWriter -> !tableWriter.getPartitioningScheme().isPresent())
            .with(source().matching(union().capturedAs(CHILD)));

    @Override
    public Pattern<TableWriterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPushTableWriteThroughUnion(session);
    }

    @Override
    public Result apply(TableWriterNode writerNode, Captures captures, Context context)
    {
        UnionNode unionNode = captures.get(CHILD);
        ImmutableList.Builder<PlanNode> rewrittenSources = ImmutableList.builder();
        List<Map<Symbol, Symbol>> sourceMappings = new ArrayList<>();
        for (int source = 0; source < unionNode.getSources().size(); source++) {
            rewrittenSources.add(rewriteSource(writerNode, unionNode, source, sourceMappings, context));
        }

        ImmutableListMultimap.Builder<Symbol, Symbol> unionMappings = ImmutableListMultimap.builder();
        sourceMappings.forEach(mappings -> mappings.forEach(unionMappings::put));

        return Result.ofPlanNode(
                new UnionNode(
                        context.getIdAllocator().getNextId(),
                        rewrittenSources.build(),
                        unionMappings.build(),
                        ImmutableList.copyOf(unionMappings.build().keySet())));
    }

    private static TableWriterNode rewriteSource(
            TableWriterNode writerNode,
            UnionNode unionNode,
            int source,
            List<Map<Symbol, Symbol>> sourceMappings,
            Context context)
    {
        Map<Symbol, Symbol> inputMappings = getInputSymbolMapping(unionNode, source);
        ImmutableMap.Builder<Symbol, Symbol> mappings = ImmutableMap.builder();
        mappings.putAll(inputMappings);
        ImmutableMap.Builder<Symbol, Symbol> outputMappings = ImmutableMap.builder();
        for (Symbol outputSymbol : writerNode.getOutputSymbols()) {
            if (inputMappings.containsKey(outputSymbol)) {
                outputMappings.put(outputSymbol, inputMappings.get(outputSymbol));
            }
            else {
                Symbol newSymbol = context.getSymbolAllocator().newSymbol(outputSymbol);
                outputMappings.put(outputSymbol, newSymbol);
                mappings.put(outputSymbol, newSymbol);
            }
        }
        sourceMappings.add(outputMappings.build());
        SymbolMapper symbolMapper = createSymbolMapper(mappings.build());
        return symbolMapper.map(writerNode, unionNode.getSources().get(source), context.getIdAllocator().getNextId());
    }

    private static Map<Symbol, Symbol> getInputSymbolMapping(UnionNode node, int source)
    {
        return node.getSymbolMapping()
                .keySet()
                .stream()
                .collect(toImmutableMap(key -> key, key -> node.getSymbolMapping().get(key).get(source)));
    }
}
