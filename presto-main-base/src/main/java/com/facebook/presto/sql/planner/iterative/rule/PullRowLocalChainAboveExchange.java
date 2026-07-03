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
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getPullRowLocalChainAboveExchangeStrategy;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PullRowLocalChainAboveExchangeStrategy.DISABLED;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.Patterns.exchange;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Pulls a maximal chain of row-local operators (deterministic {@link ProjectNode}s and
 * {@link UnnestNode}s) above a repartitioning remote {@link ExchangeNode}, so the exchange
 * shuffles the smaller pre-expansion input instead of the post-expansion (fanned-out / widened)
 * rows.
 * <p>
 * Transforms:
 * <pre>
 *  Exchange(REPARTITION, K)
 *    [Project? / Unnest]*           (chain; contains at least one Unnest)
 *      Barrier(Join / Aggregation / TableScan / Exchange / ...)
 * </pre>
 * to:
 * <pre>
 *  [Project? / Unnest]*             (same chain, reparented)
 *    Exchange(REPARTITION, K)       (now shuffles the barrier output)
 *      Barrier
 * </pre>
 * The rewrite is semantics-preserving when every partitioning / hash / ordering variable {@code K}
 * is produced unchanged by the barrier (i.e. passes through the chain). Since {@code UNNEST} only
 * fans out its array/map elements and the projections are deterministic and row-local, applying the
 * chain after the shuffle yields the same multiset of output rows.
 * <p>
 * Gated behind the {@code pull_row_local_chain_above_exchange_strategy} session property (default
 * {@code DISABLED}); for {@code ALWAYS_ENABLED} (and, for now, {@code COST_BASED}) it fires whenever
 * the rewrite is legal, independent of cost/statistics. Cost-based selection can be layered on
 * separately later under {@code COST_BASED}.
 * Generalizes {@link PullConstantProjectionAboveExchange} (which pulls only constant projection
 * assignments above an exchange) and subsumes pushing a lone {@code UNNEST} below a repartition.
 */
public class PullRowLocalChainAboveExchange
        implements Rule<ExchangeNode>
{
    private static final Pattern<ExchangeNode> PATTERN = exchange()
            .matching(exchange -> exchange.getType() == REPARTITION
                    && exchange.getScope().isRemote()
                    && !exchange.getPartitioningScheme().isReplicateNullsAndAny());

    private final RowExpressionDeterminismEvaluator determinismEvaluator;

    public PullRowLocalChainAboveExchange(FunctionAndTypeManager functionAndTypeManager)
    {
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(requireNonNull(functionAndTypeManager, "functionAndTypeManager is null"));
    }

    @Override
    public Pattern<ExchangeNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        // Fire when the strategy is not DISABLED. ALWAYS_ENABLED and COST_BASED both currently apply
        // the rewrite whenever it is legal; cost-based selection is a separate layer for later.
        return getPullRowLocalChainAboveExchangeStrategy(session) != DISABLED;
    }

    @Override
    public Result apply(ExchangeNode exchange, Captures captures, Context context)
    {
        // Single-source exchange only (a chain feeds exactly one exchange input).
        if (exchange.getSources().size() != 1) {
            return Result.empty();
        }

        // Only handle exchanges that do not rename their input columns; otherwise the reparented
        // chain would not produce the variables the consumer above the exchange expects.
        List<VariableReferenceExpression> outputLayout = exchange.getPartitioningScheme().getOutputLayout();
        if (!exchange.getInputs().get(0).equals(outputLayout)) {
            return Result.empty();
        }

        // Walk down the maximal chain of row-local operators (deterministic projects, unnests).
        List<PlanNode> chain = new ArrayList<>();
        boolean hasUnnest = false;
        PlanNode current = context.getLookup().resolve(exchange.getSources().get(0));
        while (true) {
            if (current instanceof UnnestNode) {
                chain.add(current);
                hasUnnest = true;
                current = context.getLookup().resolve(((UnnestNode) current).getSource());
            }
            else if (current instanceof ProjectNode) {
                ProjectNode project = (ProjectNode) current;
                // A non-deterministic expression must not move across the exchange; treat such a
                // projection as the barrier (it stays below the exchange, computed once).
                if (!project.getAssignments().getExpressions().stream().allMatch(determinismEvaluator::isDeterministic)) {
                    break;
                }
                chain.add(current);
                current = context.getLookup().resolve(project.getSource());
            }
            else {
                break;
            }
        }
        PlanNode barrier = current;

        // Require at least one unnest in the chain. Pulling a pure-projection chain up is the job of
        // PullConstantProjectionAboveExchange / PushProjectionThroughExchange; requiring an unnest
        // keeps this rule focused on the data-expansion case and avoids fighting those rules.
        if (!hasUnnest) {
            return Result.empty();
        }

        // The exchange's partitioning / hash / ordering variables must be produced by the barrier
        // (i.e. pass through the chain unchanged) so the exchange can be applied below the chain.
        List<VariableReferenceExpression> barrierOutputs = barrier.getOutputVariables();
        Set<VariableReferenceExpression> requiredByExchange = new HashSet<>(exchange.getPartitioningScheme().getPartitioning().getVariableReferences());
        exchange.getPartitioningScheme().getHashColumn().ifPresent(requiredByExchange::add);
        exchange.getOrderingScheme().ifPresent(ordering -> requiredByExchange.addAll(ordering.getOrderByVariables()));
        if (!barrierOutputs.containsAll(requiredByExchange)) {
            return Result.empty();
        }

        // Narrow the exchange output (correct by construction, not relying on a later prune pass) to
        // only the barrier columns the chain actually consumes plus the partitioning / hash / ordering
        // variables (K) — so the exchange shuffles only the fields that must cross it.
        Set<VariableReferenceExpression> chainReferencedVariables = new HashSet<>();
        for (PlanNode chainNode : chain) {
            if (chainNode instanceof UnnestNode) {
                UnnestNode unnest = (UnnestNode) chainNode;
                chainReferencedVariables.addAll(unnest.getReplicateVariables());
                chainReferencedVariables.addAll(unnest.getUnnestVariables().keySet());
            }
            else {
                chainReferencedVariables.addAll(VariablesExtractor.extractUnique(((ProjectNode) chainNode).getAssignments().getExpressions()));
            }
        }
        List<VariableReferenceExpression> exchangeOutputLayout = barrierOutputs.stream()
                .filter(variable -> chainReferencedVariables.contains(variable) || requiredByExchange.contains(variable))
                .collect(toImmutableList());

        // Build the exchange below the chain: same partitioning, narrowed output, single identity source.
        PartitioningScheme oldScheme = exchange.getPartitioningScheme();
        PartitioningScheme newScheme = new PartitioningScheme(
                oldScheme.getPartitioning(),
                exchangeOutputLayout,
                oldScheme.getHashColumn(),
                oldScheme.isReplicateNullsAndAny(),
                oldScheme.isScaleWriters(),
                oldScheme.getEncoding(),
                oldScheme.getBucketToPartition());
        ExchangeNode newExchange = new ExchangeNode(
                exchange.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                exchange.getType(),
                exchange.getScope(),
                newScheme,
                ImmutableList.of(barrier),
                ImmutableList.of(exchangeOutputLayout),
                exchange.isEnsureSourceOrdering(),
                exchange.getOrderingScheme());

        // Reparent the chain on top of the new exchange, bottom-up, allocating a fresh PlanNodeId for
        // each relocated node (never reuse the original plan node ids).
        PlanNode result = newExchange;
        for (int i = chain.size() - 1; i >= 0; i--) {
            result = withNewIdAndSource(chain.get(i), result, context);
        }
        return Result.ofPlanNode(result);
    }

    private static PlanNode withNewIdAndSource(PlanNode node, PlanNode newSource, Context context)
    {
        if (node instanceof UnnestNode) {
            UnnestNode unnest = (UnnestNode) node;
            return new UnnestNode(
                    unnest.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    newSource,
                    unnest.getReplicateVariables(),
                    unnest.getUnnestVariables(),
                    unnest.getOrdinalityVariable());
        }
        if (node instanceof ProjectNode) {
            ProjectNode project = (ProjectNode) node;
            return new ProjectNode(
                    project.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    newSource,
                    project.getAssignments(),
                    project.getLocality());
        }
        throw new IllegalArgumentException(format("Unexpected node type in row-local chain: %s", node.getClass().getName()));
    }
}
