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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Partitioning;
import io.prestosql.sql.planner.PartitioningScheme;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.QualifiedName;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.SystemSessionProperties.getTaskConcurrency;
import static io.prestosql.matching.Pattern.empty;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.prestosql.sql.planner.plan.Patterns.Aggregation.groupingColumns;
import static io.prestosql.sql.planner.plan.Patterns.Aggregation.step;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;

/**
 * Adds INTERMEDIATE aggregations between an un-grouped FINAL aggregation and its preceding
 * PARTIAL aggregation.
 * <p>
 * From:
 * <pre>
 * - Aggregation (FINAL)
 *   - RemoteExchange (GATHER)
 *     - Aggregation (PARTIAL)
 * </pre>
 * To:
 * <pre>
 * - Aggregation (FINAL)
 *   - LocalExchange (GATHER)
 *     - Aggregation (INTERMEDIATE)
 *       - LocalExchange (ARBITRARY)
 *         - RemoteExchange (GATHER)
 *           - Aggregation (INTERMEDIATE)
 *             - LocalExchange (GATHER)
 *               - Aggregation (PARTIAL)
 * </pre>
 * <p>
 */
public class AddIntermediateAggregations
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            // Only consider FINAL un-grouped aggregations
            .with(step().equalTo(AggregationNode.Step.FINAL))
            .with(empty(groupingColumns()))
            // Only consider aggregations without ORDER BY clause
            .matching(node -> !node.hasOrderings());

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return SystemSessionProperties.isEnableIntermediateAggregations(session);
    }

    @Override
    public Result apply(AggregationNode aggregation, Captures captures, Context context)
    {
        Lookup lookup = context.getLookup();
        PlanNodeIdAllocator idAllocator = context.getIdAllocator();
        Session session = context.getSession();

        Optional<PlanNode> rewrittenSource = recurseToPartial(lookup.resolve(aggregation.getSource()), lookup, idAllocator);

        if (!rewrittenSource.isPresent()) {
            return Result.empty();
        }

        PlanNode source = rewrittenSource.get();

        if (getTaskConcurrency(session) > 1) {
            source = ExchangeNode.partitionedExchange(
                    idAllocator.getNextId(),
                    ExchangeNode.Scope.LOCAL,
                    source,
                    new PartitioningScheme(Partitioning.create(FIXED_ARBITRARY_DISTRIBUTION, ImmutableList.of()), source.getOutputSymbols()));
            source = new AggregationNode(
                    idAllocator.getNextId(),
                    source,
                    inputsAsOutputs(aggregation.getAggregations()),
                    aggregation.getGroupingSets(),
                    aggregation.getPreGroupedSymbols(),
                    AggregationNode.Step.INTERMEDIATE,
                    aggregation.getHashSymbol(),
                    aggregation.getGroupIdSymbol());
            source = ExchangeNode.gatheringExchange(idAllocator.getNextId(), ExchangeNode.Scope.LOCAL, source);
        }

        return Result.ofPlanNode(aggregation.replaceChildren(ImmutableList.of(source)));
    }

    /**
     * Recurse through a series of preceding ExchangeNodes and ProjectNodes to find the preceding PARTIAL aggregation
     */
    private Optional<PlanNode> recurseToPartial(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator)
    {
        if (node instanceof AggregationNode && ((AggregationNode) node).getStep() == AggregationNode.Step.PARTIAL) {
            return Optional.of(addGatheringIntermediate((AggregationNode) node, idAllocator));
        }

        if (!(node instanceof ExchangeNode) && !(node instanceof ProjectNode)) {
            return Optional.empty();
        }

        ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
        for (PlanNode source : node.getSources()) {
            Optional<PlanNode> planNode = recurseToPartial(lookup.resolve(source), lookup, idAllocator);
            if (!planNode.isPresent()) {
                return Optional.empty();
            }
            builder.add(planNode.get());
        }
        return Optional.of(node.replaceChildren(builder.build()));
    }

    private PlanNode addGatheringIntermediate(AggregationNode aggregation, PlanNodeIdAllocator idAllocator)
    {
        verify(aggregation.getGroupingKeys().isEmpty(), "Should be an un-grouped aggregation");
        ExchangeNode gatheringExchange = ExchangeNode.gatheringExchange(idAllocator.getNextId(), ExchangeNode.Scope.LOCAL, aggregation);
        return new AggregationNode(
                idAllocator.getNextId(),
                gatheringExchange,
                outputsAsInputs(aggregation.getAggregations()),
                aggregation.getGroupingSets(),
                aggregation.getPreGroupedSymbols(),
                AggregationNode.Step.INTERMEDIATE,
                aggregation.getHashSymbol(),
                aggregation.getGroupIdSymbol());
    }

    /**
     * Rewrite assignments so that inputs are in terms of the output symbols.
     * <p>
     * Example:
     * 'a' := sum('b') => 'a' := sum('a')
     * 'a' := count(*) => 'a' := count('a')
     */
    private static Map<Symbol, AggregationNode.Aggregation> outputsAsInputs(Map<Symbol, AggregationNode.Aggregation> assignments)
    {
        ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> builder = ImmutableMap.builder();
        for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : assignments.entrySet()) {
            Symbol output = entry.getKey();
            AggregationNode.Aggregation aggregation = entry.getValue();
            checkState(!aggregation.getCall().getOrderBy().isPresent(), "Intermediate aggregation does not support ORDER BY");
            builder.put(
                    output,
                    new AggregationNode.Aggregation(
                            new FunctionCall(QualifiedName.of(aggregation.getSignature().getName()), ImmutableList.of(output.toSymbolReference())),
                            aggregation.getSignature(),
                            Optional.empty()));  // No mask for INTERMEDIATE
        }
        return builder.build();
    }

    /**
     * Rewrite assignments so that outputs are in terms of the input symbols.
     * This operation only reliably applies to aggregation steps that take partial inputs (e.g. INTERMEDIATE and split FINALs),
     * which are guaranteed to have exactly one input and one output.
     * <p>
     * Example:
     * 'a' := sum('b') => 'b' := sum('b')
     */
    private static Map<Symbol, AggregationNode.Aggregation> inputsAsOutputs(Map<Symbol, AggregationNode.Aggregation> assignments)
    {
        ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> builder = ImmutableMap.builder();
        for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : assignments.entrySet()) {
            // Should only have one input symbol
            Symbol input = getOnlyElement(SymbolsExtractor.extractAll(entry.getValue().getCall()));
            builder.put(input, entry.getValue());
        }
        return builder.build();
    }
}
