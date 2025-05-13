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
package com.facebook.presto.spi.plan;

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.common.Utils.checkArgument;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class SimplePlanFragment
{
    private final PlanFragmentId id;
    private final PlanNode root;
    private final Set<VariableReferenceExpression> variables;
    private final PartitioningHandle partitioning;
    private final List<PlanNodeId> tableScanSchedulingOrder;
    private final PartitioningScheme partitioningScheme;
    private final StageExecutionDescriptor stageExecutionDescriptor;

    // Only true for output table writer and false for temporary table writers
    private final boolean outputTableWriterFragment;

    @JsonCreator
    public SimplePlanFragment(
            @JsonProperty("id") PlanFragmentId id,
            @JsonProperty("root") PlanNode root,
            @JsonProperty("variables") Set<VariableReferenceExpression> variables,
            @JsonProperty("partitioning") PartitioningHandle partitioning,
            @JsonProperty("tableScanSchedulingOrder") List<PlanNodeId> tableScanSchedulingOrder,
            @JsonProperty("partitioningScheme") PartitioningScheme partitioningScheme,
            @JsonProperty("stageExecutionDescriptor") StageExecutionDescriptor stageExecutionDescriptor,
            @JsonProperty("outputTableWriterFragment") boolean outputTableWriterFragment)
    {
        this.id = requireNonNull(id, "id is null");
        this.root = requireNonNull(root, "root is null");
        this.variables = requireNonNull(variables, "variables is null");
        this.partitioning = requireNonNull(partitioning, "partitioning is null");
        requireNonNull(tableScanSchedulingOrder, "tableScanSchedulingOrder is null");
        this.tableScanSchedulingOrder = unmodifiableList(new ArrayList<>(tableScanSchedulingOrder));
        this.stageExecutionDescriptor = requireNonNull(stageExecutionDescriptor, "stageExecutionDescriptor is null");
        this.outputTableWriterFragment = outputTableWriterFragment;

        requireNonNull(partitioningScheme, "partitioningScheme is null");
        checkArgument(new HashSet<>(root.getOutputVariables()).containsAll(partitioningScheme.getOutputLayout()),
                format("Root node outputs (%s) does not include all fragment outputs (%s)", root.getOutputVariables(), partitioningScheme.getOutputLayout()));
        this.partitioningScheme = partitioningScheme;
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
}
