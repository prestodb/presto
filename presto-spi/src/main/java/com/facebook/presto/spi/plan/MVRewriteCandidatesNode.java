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

import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

/**
 * A plan node that holds the logical plan for the original query along with
 * all materialized view rewrite candidate plans. This is used for cost-based
 * MV selection where the optimizer will choose the best plan based on cost.
 */
@Immutable
public final class MVRewriteCandidatesNode
        extends PlanNode
{
    private final PlanNode originalPlan;
    private final List<MVRewriteCandidate> candidates;
    private final List<VariableReferenceExpression> outputVariables;

    @JsonCreator
    public MVRewriteCandidatesNode(
            @JsonProperty("location") Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("originalPlan") PlanNode originalPlan,
            @JsonProperty("candidates") List<MVRewriteCandidate> candidates,
            @JsonProperty("outputVariables") List<VariableReferenceExpression> outputVariables)
    {
        this(sourceLocation, id, Optional.empty(), originalPlan, candidates, outputVariables);
    }

    public MVRewriteCandidatesNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            PlanNode originalPlan,
            List<MVRewriteCandidate> candidates,
            List<VariableReferenceExpression> outputVariables)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);
        this.originalPlan = requireNonNull(originalPlan, "originalPlan is null");
        this.candidates = unmodifiableList(new ArrayList<>(requireNonNull(candidates, "candidates is null")));
        this.outputVariables = unmodifiableList(new ArrayList<>(requireNonNull(outputVariables, "outputVariables is null")));
    }

    @JsonProperty
    public PlanNode getOriginalPlan()
    {
        return originalPlan;
    }

    @JsonProperty
    public List<MVRewriteCandidate> getCandidates()
    {
        return candidates;
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
        List<PlanNode> sources = new ArrayList<>();
        sources.add(originalPlan);
        for (MVRewriteCandidate candidate : candidates) {
            sources.add(candidate.getPlan());
        }
        return unmodifiableList(sources);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitMVRewriteCandidates(this, context);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new MVRewriteCandidatesNode(getSourceLocation(), getId(), statsEquivalentPlanNode, originalPlan, candidates, outputVariables);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        if (newChildren.size() != candidates.size() + 1) {
            throw new IllegalArgumentException("Expected " + (candidates.size() + 1) + " children, but got " + newChildren.size());
        }

        PlanNode newOriginalPlan = newChildren.get(0);
        List<MVRewriteCandidate> newCandidates = new ArrayList<>();
        for (int i = 0; i < candidates.size(); i++) {
            MVRewriteCandidate oldCandidate = candidates.get(i);
            newCandidates.add(new MVRewriteCandidate(
                    newChildren.get(i + 1),
                    oldCandidate.getMaterializedViewCatalog(),
                    oldCandidate.getMaterializedViewSchema(),
                    oldCandidate.getMaterializedViewName()));
        }

        return new MVRewriteCandidatesNode(getSourceLocation(), getId(), getStatsEquivalentPlanNode(), newOriginalPlan, newCandidates, outputVariables);
    }

    /**
     * Represents a single materialized view rewrite candidate with its
     * logical plan and MV metadata.
     */
    @Immutable
    public static final class MVRewriteCandidate
    {
        private final PlanNode plan;
        private final String materializedViewCatalog;
        private final String materializedViewSchema;
        private final String materializedViewName;

        @JsonCreator
        public MVRewriteCandidate(
                @JsonProperty("plan") PlanNode plan,
                @JsonProperty("materializedViewCatalog") String materializedViewCatalog,
                @JsonProperty("materializedViewSchema") String materializedViewSchema,
                @JsonProperty("materializedViewName") String materializedViewName)
        {
            this.plan = requireNonNull(plan, "plan is null");
            this.materializedViewCatalog = requireNonNull(materializedViewCatalog, "materializedViewCatalog is null");
            this.materializedViewSchema = requireNonNull(materializedViewSchema, "materializedViewSchema is null");
            this.materializedViewName = requireNonNull(materializedViewName, "materializedViewName is null");
        }

        @JsonProperty
        public PlanNode getPlan()
        {
            return plan;
        }

        @JsonProperty
        public String getMaterializedViewCatalog()
        {
            return materializedViewCatalog;
        }

        @JsonProperty
        public String getMaterializedViewSchema()
        {
            return materializedViewSchema;
        }

        @JsonProperty
        public String getMaterializedViewName()
        {
            return materializedViewName;
        }

        public String getFullyQualifiedName()
        {
            return materializedViewCatalog + "." + materializedViewSchema + "." + materializedViewName;
        }
    }
}
