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
import com.facebook.presto.sql.planner.plan.AbstractJoinNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.ArrayList;
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
    private final Multimap<String, DynamicFilterPlaceholder> probeVariables;

    // Mapping from dynamic filter ID to its build channel indices.
    private final Map<String, Integer> buildChannels;

    private final SettableFuture<TupleDomain<VariableReferenceExpression>> resultFuture;

    // Number of build-side partitions to be collected.
    private final int partitionCount;

    // The resulting predicates from each build-side partition.
    private final List<TupleDomain<String>> partitions;

    public LocalDynamicFilter(Multimap<String, DynamicFilterPlaceholder> probeVariables, Map<String, Integer> buildChannels, int partitionCount)
    {
        this.probeVariables = requireNonNull(probeVariables, "probeVariables is null");
        this.buildChannels = requireNonNull(buildChannels, "buildChannels is null");
        verify(probeVariables.keySet().equals(buildChannels.keySet()), "probeVariables and buildChannels must have same keys");

        this.resultFuture = SettableFuture.create();

        this.partitionCount = partitionCount;
        this.partitions = new ArrayList<>(partitionCount);
    }

    private synchronized void addPartition(TupleDomain<String> tupleDomain)
    {
        // Called concurrently by each DynamicFilterSourceOperator instance (when collection is over).
        verify(partitions.size() < partitionCount);
        // NOTE: may result in a bit more relaxed constraint if there are multiple columns and multiple rows.
        // See the comment at TupleDomain::columnWiseUnion() for more details.
        partitions.add(tupleDomain);
        if (partitions.size() == partitionCount) {
            // No more partitions are left to be processed.
            TupleDomain<VariableReferenceExpression> result = convertTupleDomain(TupleDomain.columnWiseUnion(partitions));
            verify(resultFuture.set(result), "dynamic filter result is provided more than once");
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
            for (DynamicFilterPlaceholder placeholder : probeVariables.get(entry.getKey())) {
                Domain updatedDomain = placeholder.applyComparison(domain);
                builder.put((VariableReferenceExpression) placeholder.getInput(), updatedDomain);
            }
        }
        return TupleDomain.withColumnDomains(builder.build());
    }

    public static Optional<LocalDynamicFilter> create(AbstractJoinNode planNode, int partitionCount)
    {
        Set<String> joinDynamicFilters = planNode.getDynamicFilters().keySet();
        List<FilterNode> filterNodes = PlanNodeSearcher
                .searchFrom(planNode.getProbe())
                .where(LocalDynamicFilter::isFilterAboveTableScan)
                .findAll();

        // Mapping from probe-side dynamic filters' IDs to their matching probe variables.
        ImmutableMultimap.Builder<String, DynamicFilterPlaceholder> probeVariablesBuilder = ImmutableMultimap.builder();
        for (FilterNode filterNode : filterNodes) {
            DynamicFilterExtractResult extractResult = extractDynamicFilters(filterNode.getPredicate());
            for (DynamicFilterPlaceholder placeholder : extractResult.getDynamicConjuncts()) {
                if (placeholder.getInput() instanceof VariableReferenceExpression) {
                    // Add descriptors that match the local dynamic filter (from the current join node).
                    if (joinDynamicFilters.contains(placeholder.getId())) {
                        probeVariablesBuilder.put(placeholder.getId(), placeholder);
                    }
                }
            }
        }

        Multimap<String, DynamicFilterPlaceholder> probeVariables = probeVariablesBuilder.build();
        PlanNode buildNode = planNode.getBuild();
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
                .add("partitionCount", partitionCount)
                .add("partitions", partitions)
                .toString();
    }
}
