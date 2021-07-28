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

import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class HdfsFinishNode
        extends InternalPlanNode
{
    private final PlanNode source;
    private final Optional<String> schema;
    private final Optional<VariableReferenceExpression> schemaVariable;
    private final VariableReferenceExpression rowCountVariable;

    @JsonCreator
    public HdfsFinishNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("schemaVariable") Optional<VariableReferenceExpression> schemaVariable,
            @JsonProperty("rowCountVariable") VariableReferenceExpression rowCountVariable)
    {
        super(id);
        this.source = requireNonNull(source, "source is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.schemaVariable = requireNonNull(schemaVariable, "schemaVariable is null");
        this.rowCountVariable = requireNonNull(rowCountVariable, "rowCountVariable is null");
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public Optional<String> getSchema()
    {
        return schema;
    }

    @JsonProperty
    public Optional<VariableReferenceExpression> getSchemaVariable()
    {
        return schemaVariable;
    }

    @JsonProperty
    public VariableReferenceExpression getRowCountVariable()
    {
        return rowCountVariable;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return schemaVariable.isPresent() ? ImmutableList.of(rowCountVariable, schemaVariable.get()) : ImmutableList.of(rowCountVariable);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new HdfsFinishNode(
                getId(),
                Iterables.getOnlyElement(newChildren),
                schema,
                schemaVariable,
                rowCountVariable);
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitHdfsFinish(this, context);
    }
}
