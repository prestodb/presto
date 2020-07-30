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
package com.facebook.presto.sql.planner;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.expressions.DynamicFilters.DynamicFilterExtractResult;
import com.facebook.presto.expressions.DynamicFilters.DynamicFilterPlaceholder;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static com.facebook.presto.expressions.DynamicFilters.extractDynamicFilters;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class LocalDynamicFilter
{
    // Mapping from dynamic filter ID to its probe variables.
    private final Multimap<String, VariableReferenceExpression> probeVariables;

    // Mapping from dynamic filter ID to its build channel indices.
    private final Map<String, Integer> buildChannels;

    private final SettableFuture<TupleDomain<VariableReferenceExpression>> resultFuture;

    // The resulting predicate for local dynamic filtering.
    private TupleDomain<String> result;

    // Number of partitions left to be processed.
    private int partitionsLeft;

    public LocalDynamicFilter(Multimap<String, VariableReferenceExpression> probeVariables, Map<String, Integer> buildChannels, int partitionCount)
    {
        this.probeVariables = requireNonNull(probeVariables, "probeVariables is null");
        this.buildChannels = requireNonNull(buildChannels, "buildChannels is null");
        verify(probeVariables.keySet().equals(buildChannels.keySet()), "probeVariables and buildChannels must have same keys");

        this.resultFuture = SettableFuture.create();

        this.result = TupleDomain.none();
        this.partitionsLeft = partitionCount;
    }

    private synchronized void addPartition(TupleDomain<String> tupleDomain)
    {
        // Called concurrently by each DynamicFilterSourceOperator instance (when collection is over).
        partitionsLeft -= 1;
        verify(partitionsLeft >= 0);
        // NOTE: may result in a bit more relaxed constraint if there are multiple columns and multiple rows.
        // See the comment at TupleDomain::columnWiseUnion() for more details.
        result = TupleDomain.columnWiseUnion(result, tupleDomain);
        if (partitionsLeft == 0) {
            // No more partitions are left to be processed.
            verify(resultFuture.set(convertTupleDomain(result)), "dynamic filter result is provided more than once");
        }
    }

    private TupleDomain<VariableReferenceExpression> convertTupleDomain(TupleDomain<String> result)
    {
        if (result.isNone()) {
            return TupleDomain.none();
        }
        // Convert the predicate to use probe variables (instead dynamic filter IDs).
        // Note that in case of a probe-side union, a single dynamic filter may match multiple probe variables.
        ImmutableMap.Builder<VariableReferenceExpression, Domain> builder = ImmutableMap.builder();
        for (Map.Entry<String, Domain> entry : result.getDomains().get().entrySet()) {
            Domain domain = entry.getValue();
            // Store all matching variables for each build channel index.
            for (VariableReferenceExpression probeVariable : probeVariables.get(entry.getKey())) {
                builder.put(probeVariable, domain);
            }
        }
        return TupleDomain.withColumnDomains(builder.build());
    }

    public static Optional<LocalDynamicFilter> create(JoinNode planNode, int partitionCount)
    {
        Set<String> joinDynamicFilters = planNode.getDynamicFilters().keySet();
        List<FilterNode> filterNodes = PlanNodeSearcher
                .searchFrom(planNode.getLeft())
                .where(LocalDynamicFilter::isFilterAboveTableScan)
                .findAll();

        // Mapping from probe-side dynamic filters' IDs to their matching probe variables.
        ImmutableMultimap.Builder<String, VariableReferenceExpression> probeVariablesBuilder = ImmutableMultimap.builder();
        for (FilterNode filterNode : filterNodes) {
            DynamicFilterExtractResult extractResult = extractDynamicFilters(filterNode.getPredicate());
            for (DynamicFilterPlaceholder placeholder : extractResult.getDynamicConjuncts()) {
                if (placeholder.getInput() instanceof VariableReferenceExpression) {
                    // Add descriptors that match the local dynamic filter (from the current join node).
                    if (joinDynamicFilters.contains(placeholder.getId())) {
                        VariableReferenceExpression probeVariable = (VariableReferenceExpression) placeholder.getInput();
                        probeVariablesBuilder.put(placeholder.getId(), probeVariable);
                    }
                }
            }
        }

        Multimap<String, VariableReferenceExpression> probeVariables = probeVariablesBuilder.build();
        PlanNode buildNode = planNode.getRight();
        Map<String, Integer> buildChannels = planNode.getDynamicFilters().entrySet().stream()
                // Skip build channels that don't match local probe dynamic filters.
                .filter(entry -> probeVariables.containsKey(entry.getKey()))
                .collect(toMap(
                        // Dynamic filter ID
                        Map.Entry::getKey,
                        // Build-side channel index
                        entry -> {
                            VariableReferenceExpression buildVariable = entry.getValue();
                            int buildChannelIndex = buildNode.getOutputVariables().indexOf(buildVariable);
                            verify(buildChannelIndex >= 0);
                            return buildChannelIndex;
                        }));

        if (buildChannels.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new LocalDynamicFilter(probeVariables, buildChannels, partitionCount));
    }

    private static boolean isFilterAboveTableScan(PlanNode node)
    {
        return node instanceof FilterNode && ((FilterNode) node).getSource() instanceof TableScanNode;
    }

    public Map<String, Integer> getBuildChannels()
    {
        return buildChannels;
    }

    public ListenableFuture<TupleDomain<VariableReferenceExpression>> getResultFuture()
    {
        return resultFuture;
    }

    public Consumer<TupleDomain<String>> getTupleDomainConsumer()
    {
        return this::addPartition;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("probeVariables", probeVariables)
                .add("buildChannels", buildChannels)
                .add("result", result)
                .add("partitionsLeft", partitionsLeft)
                .toString();
    }
}
