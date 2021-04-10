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

import com.facebook.airlift.json.Codec;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.operator.StageExecutionDescriptor;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class PlanFragment
{
    private final PlanFragmentId id;
    private final PlanNode root;
    private final Set<VariableReferenceExpression> variables;
    private final PartitioningHandle partitioning;
    private final List<PlanNodeId> tableScanSchedulingOrder;
    private final List<Type> types;
    private final List<RemoteSourceNode> remoteSourceNodes;
    private final PartitioningScheme partitioningScheme;
    private final StageExecutionDescriptor stageExecutionDescriptor;
    private final boolean outputTableWriterFragment;
    private final StatsAndCosts statsAndCosts;
    private final Optional<String> jsonRepresentation;

    // This is ensured to be lazily populated on the first successful call to #toBytes
    @GuardedBy("this")
    private byte[] cachedSerialization;
    @GuardedBy("this")
    private Codec<PlanFragment> lastUsedCodec;

    @JsonCreator
    public PlanFragment(
            @JsonProperty("id") PlanFragmentId id,
            @JsonProperty("root") PlanNode root,
            @JsonProperty("variables") Set<VariableReferenceExpression> variables,
            @JsonProperty("partitioning") PartitioningHandle partitioning,
            @JsonProperty("tableScanSchedulingOrder") List<PlanNodeId> tableScanSchedulingOrder,
            @JsonProperty("partitioningScheme") PartitioningScheme partitioningScheme,
            @JsonProperty("stageExecutionDescriptor") StageExecutionDescriptor stageExecutionDescriptor,
            @JsonProperty("outputTableWriterFragment") boolean outputTableWriterFragment,
            @JsonProperty("statsAndCosts") StatsAndCosts statsAndCosts,
            @JsonProperty("jsonRepresentation") Optional<String> jsonRepresentation)
    {
        this.id = requireNonNull(id, "id is null");
        this.root = requireNonNull(root, "root is null");
        this.variables = requireNonNull(variables, "variables is null");
        this.partitioning = requireNonNull(partitioning, "partitioning is null");
        this.tableScanSchedulingOrder = ImmutableList.copyOf(requireNonNull(tableScanSchedulingOrder, "tableScanSchedulingOrder is null"));
        this.stageExecutionDescriptor = requireNonNull(stageExecutionDescriptor, "stageExecutionDescriptor is null");
        this.outputTableWriterFragment = outputTableWriterFragment;
        this.statsAndCosts = requireNonNull(statsAndCosts, "statsAndCosts is null");
        this.jsonRepresentation = requireNonNull(jsonRepresentation, "jsonRepresentation is null");

        checkArgument(root.getOutputVariables().containsAll(partitioningScheme.getOutputLayout()),
                "Root node outputs (%s) does not include all fragment outputs (%s)", root.getOutputVariables(), partitioningScheme.getOutputLayout());

        types = partitioningScheme.getOutputLayout().stream()
                .map(VariableReferenceExpression::getType)
                .collect(toImmutableList());

        ImmutableList.Builder<RemoteSourceNode> remoteSourceNodes = ImmutableList.builder();
        findRemoteSourceNodes(root, remoteSourceNodes);
        this.remoteSourceNodes = remoteSourceNodes.build();

        this.partitioningScheme = requireNonNull(partitioningScheme, "partitioningScheme is null");
    }

    @JsonProperty
    public PlanFragmentId getId()
    {
        return id;
    }

    @JsonProperty
    public PlanNode getRoot()
    {
        return root;
    }

    @JsonProperty
    public Set<VariableReferenceExpression> getVariables()
    {
        return variables;
    }

    @JsonProperty
    public PartitioningHandle getPartitioning()
    {
        return partitioning;
    }

    @JsonProperty
    public List<PlanNodeId> getTableScanSchedulingOrder()
    {
        return tableScanSchedulingOrder;
    }

    @JsonProperty
    public PartitioningScheme getPartitioningScheme()
    {
        return partitioningScheme;
    }

    @JsonProperty
    public StageExecutionDescriptor getStageExecutionDescriptor()
    {
        return stageExecutionDescriptor;
    }

    @JsonProperty
    public boolean isOutputTableWriterFragment()
    {
        return outputTableWriterFragment;
    }

    @JsonProperty
    public StatsAndCosts getStatsAndCosts()
    {
        return statsAndCosts;
    }

    @JsonProperty
    public Optional<String> getJsonRepresentation()
    {
        // @reviewer: I believe this should be a json raw value, but that would make this class have a different deserialization constructor.
        // workers don't need this, so that should be OK, but it's worth thinking about.
        return jsonRepresentation;
    }

    // Serialize this plan fragment with the provided codec, caching the results
    public synchronized byte[] toBytes(Codec<PlanFragment> codec)
    {
        requireNonNull(codec, "codec is null");
        if (cachedSerialization != null) {
            verify(codec == lastUsedCodec, "Only one Codec may be used to serialize PlanFragments");
        }
        else {
            cachedSerialization = codec.toBytes(this);
            lastUsedCodec = codec;
        }
        return cachedSerialization;
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public boolean isLeaf()
    {
        return remoteSourceNodes.isEmpty();
    }

    public List<RemoteSourceNode> getRemoteSourceNodes()
    {
        return remoteSourceNodes;
    }

    private static Set<PlanNode> findSources(PlanNode node, Iterable<PlanNodeId> nodeIds)
    {
        ImmutableSet.Builder<PlanNode> nodes = ImmutableSet.builder();
        findSources(node, ImmutableSet.copyOf(nodeIds), nodes);
        return nodes.build();
    }

    private static void findSources(PlanNode node, Set<PlanNodeId> nodeIds, ImmutableSet.Builder<PlanNode> nodes)
    {
        if (nodeIds.contains(node.getId())) {
            nodes.add(node);
        }

        for (PlanNode source : node.getSources()) {
            nodes.addAll(findSources(source, nodeIds));
        }
    }

    private static void findRemoteSourceNodes(PlanNode node, Builder<RemoteSourceNode> builder)
    {
        for (PlanNode source : node.getSources()) {
            findRemoteSourceNodes(source, builder);
        }

        if (node instanceof RemoteSourceNode) {
            builder.add((RemoteSourceNode) node);
        }
    }

    public PlanFragment withBucketToPartition(Optional<int[]> bucketToPartition)
    {
        return new PlanFragment(
                id,
                root,
                variables,
                partitioning,
                tableScanSchedulingOrder,
                partitioningScheme.withBucketToPartition(bucketToPartition),
                stageExecutionDescriptor,
                outputTableWriterFragment,
                statsAndCosts,
                jsonRepresentation);
    }

    public PlanFragment withFixedLifespanScheduleGroupedExecution(List<PlanNodeId> capableTableScanNodes, int totalLifespans)
    {
        return new PlanFragment(
                id,
                root,
                variables,
                partitioning,
                tableScanSchedulingOrder,
                partitioningScheme,
                StageExecutionDescriptor.fixedLifespanScheduleGroupedExecution(capableTableScanNodes, totalLifespans),
                outputTableWriterFragment,
                statsAndCosts,
                jsonRepresentation);
    }

    public PlanFragment withDynamicLifespanScheduleGroupedExecution(List<PlanNodeId> capableTableScanNodes, int totalLifespans)
    {
        return new PlanFragment(
                id,
                root,
                variables,
                partitioning,
                tableScanSchedulingOrder,
                partitioningScheme,
                StageExecutionDescriptor.dynamicLifespanScheduleGroupedExecution(capableTableScanNodes, totalLifespans),
                outputTableWriterFragment,
                statsAndCosts,
                jsonRepresentation);
    }

    public PlanFragment withRecoverableGroupedExecution(List<PlanNodeId> capableTableScanNodes, int totalLifespans)
    {
        return new PlanFragment(
                id,
                root,
                variables,
                partitioning,
                tableScanSchedulingOrder,
                partitioningScheme,
                StageExecutionDescriptor.recoverableGroupedExecution(capableTableScanNodes, totalLifespans),
                outputTableWriterFragment,
                statsAndCosts,
                jsonRepresentation);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("partitioning", partitioning)
                .add("tableScanSchedulingOrder", tableScanSchedulingOrder)
                .add("partitionFunction", partitioningScheme)
                .toString();
    }
}
