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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.plan.LogicalProperties;
import com.facebook.presto.spi.plan.LogicalPropertiesProvider;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AssignUniqueId
        extends InternalPlanNode
{
    private final PlanNode source;
    private final VariableReferenceExpression idVariable;

    @JsonCreator
    public AssignUniqueId(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("idVariable") VariableReferenceExpression idVariable)
    {
        super(sourceLocation, id);
        this.source = requireNonNull(source, "source is null");
        this.idVariable = requireNonNull(idVariable, "idVariable is null");
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return ImmutableList.<VariableReferenceExpression>builder()
                .addAll(source.getOutputVariables())
                .add(idVariable)
                .build();
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty
    public VariableReferenceExpression getIdVariable()
    {
        return idVariable;
    }

    @Override
    public LogicalProperties computeLogicalProperties(LogicalPropertiesProvider logicalPropertiesProvider)
    {
        requireNonNull(logicalPropertiesProvider, "logicalPropertiesProvider cannot be null.");
        return logicalPropertiesProvider.getAssignUniqueIdProperties(this);
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitAssignUniqueId(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 1, "expected newChildren to contain 1 node");
        return new AssignUniqueId(newChildren.get(0).getSourceLocation(), getId(), Iterables.getOnlyElement(newChildren), idVariable);
    }
}
