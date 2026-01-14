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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A logical plan node representing a materialized view refresh operation.
 *
 * <p>This node acts as a marker that is replaced by optimizer rules with the actual
 * execution plan (either full refresh or incremental refresh). This pattern allows
 * for cost-based optimization to choose between refresh strategies.
 *
 * <p>Similar to how TableScanNode for an MV gets replaced by MaterializedViewRewrite
 * with a UNION plan for queries, this node gets replaced with the appropriate
 * TableWriterNode structure for refresh operations.
 *
 * @see com.facebook.presto.sql.planner.iterative.rule.materializedview.IncrementalRefreshRule
 */
public final class RefreshMaterializedViewNode
        extends PlanNode
{
    private final SchemaTableName materializedViewName;
    private final TableHandle storageTableHandle;
    private final PlanNode source;
    private final List<ColumnHandle> columnHandles;
    private final List<VariableReferenceExpression> outputVariables;

    @JsonCreator
    public RefreshMaterializedViewNode(
            @JsonProperty("sourceLocation") Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("materializedViewName") SchemaTableName materializedViewName,
            @JsonProperty("storageTableHandle") TableHandle storageTableHandle,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("columnHandles") List<ColumnHandle> columnHandles,
            @JsonProperty("outputVariables") List<VariableReferenceExpression> outputVariables)
    {
        super(sourceLocation, id, Optional.empty());
        this.materializedViewName = requireNonNull(materializedViewName, "materializedViewName is null");
        this.storageTableHandle = requireNonNull(storageTableHandle, "storageTableHandle is null");
        this.source = requireNonNull(source, "source is null");
        this.columnHandles = Collections.unmodifiableList(requireNonNull(columnHandles, "columnHandles is null"));
        this.outputVariables = Collections.unmodifiableList(requireNonNull(outputVariables, "outputVariables is null"));
    }

    @JsonProperty
    public SchemaTableName getMaterializedViewName()
    {
        return materializedViewName;
    }

    @JsonProperty
    public TableHandle getStorageTableHandle()
    {
        return storageTableHandle;
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public List<ColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    @Override
    @JsonProperty
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return Collections.singletonList(source);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        if (newChildren.size() != 1) {
            throw new IllegalArgumentException("Expected exactly one child, got " + newChildren.size());
        }
        return new RefreshMaterializedViewNode(
                getSourceLocation(),
                getId(),
                materializedViewName,
                storageTableHandle,
                newChildren.get(0),
                columnHandles,
                outputVariables);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return this;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitRefreshMaterializedView(this, context);
    }
}
