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

import static java.util.Objects.requireNonNull;

@Immutable
public class ExplainAnalyzeNode
        extends InternalPlanNode
{
    private final PlanNode source;
    private final VariableReferenceExpression outputVariable;
    private final boolean verbose;

    @JsonCreator
    public ExplainAnalyzeNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("outputVariable") VariableReferenceExpression outputVariable,
            @JsonProperty("verbose") boolean verbose)
    {
        super(id);
        this.source = requireNonNull(source, "source is null");
        this.outputVariable = requireNonNull(outputVariable, "outputVariable is null");
        this.verbose = verbose;
    }

    @JsonProperty("outputVariable")
    public VariableReferenceExpression getOutputVariable()
    {
        return outputVariable;
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty("verbose")
    public boolean isVerbose()
    {
        return verbose;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return ImmutableList.of(outputVariable);
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitExplainAnalyze(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new ExplainAnalyzeNode(getId(), Iterables.getOnlyElement(newChildren), outputVariable, isVerbose());
    }
}
