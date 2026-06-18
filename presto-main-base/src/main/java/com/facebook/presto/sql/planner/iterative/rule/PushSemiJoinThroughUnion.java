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
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.RowExpressionVariableInliner;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isPushSemiJoinThroughUnion;
import static com.facebook.presto.sql.planner.optimizations.SetOperationNodeUtils.fromListMultimap;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoin;

/**
 * Pushes a SemiJoinNode through a UnionNode (on the probe/source side).
 * <p>
 * Transforms:
 * <pre>
 *     - SemiJoin (sourceJoinVar=c, output=sjOut)
 *         - Union (output c from [a1, a2])
 *             - source1 (outputs a1)
 *             - source2 (outputs a2)
 *         - filteringSource
 * </pre>
 * into:
 * <pre>
 *     - Union (output sjOut from [sjOut_0, sjOut_1], c from [a1, a2])
 *         - SemiJoin (sourceJoinVar=a1, output=sjOut_0)
 *             - source1
 *             - filteringSource
 *         - SemiJoin (sourceJoinVar=a2, output=sjOut_1)
 *             - source2
 *             - filteringSource
 * </pre>
 * <p>
 * Also handles the case where a ProjectNode sits between the SemiJoin and Union:
 * <pre>
 *     - SemiJoin
 *         - Project
 *             - Union
 *         - filteringSource
 * </pre>
 * In this case, the project is pushed into each union branch before the semi join.
 */
public class PushSemiJoinThroughUnion
        implements Rule<SemiJoinNode>
{
    private static final Pattern<SemiJoinNode> PATTERN = semiJoin();

    @Override
    public Pattern<SemiJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPushSemiJoinThroughUnion(session);
    }

    @Override
    public Result apply(SemiJoinNode semiJoinNode, Captures captures, Context context)
    {
        PlanNode source = context.getLookup().resolve(semiJoinNode.getSource());

        if (source instanceof UnionNode) {
            return pushThroughUnion(semiJoinNode, (UnionNode) source, Optional.empty(), context);
        }

        if (source instanceof ProjectNode) {
            ProjectNode projectNode = (ProjectNode) source;
            PlanNode projectSource = context.getLookup().resolve(projectNode.getSource());
            if (projectSource instanceof UnionNode) {
                return pushThroughUnion(semiJoinNode, (UnionNode) projectSource, Optional.of(projectNode), context);
            }
        }

        return Result.empty();
    }

    private Result pushThroughUnion(
            SemiJoinNode semiJoinNode,
            UnionNode unionNode,
            Optional<ProjectNode> projectNode,
            Context context)
    {
        ImmutableList.Builder<PlanNode> newSources = ImmutableList.builder();
        ImmutableListMultimap.Builder<VariableReferenceExpression, VariableReferenceExpression> outputMappings =
                ImmutableListMultimap.builder();

        for (int i = 0; i < unionNode.getSources().size(); i++) {
            Map<VariableReferenceExpression, VariableReferenceExpression> unionVarMap = unionNode.sourceVariableMap(i);

            PlanNode branchSource;
            VariableReferenceExpression mappedSourceJoinVar;
            Optional<VariableReferenceExpression> mappedSourceHashVar;
            Map<String, VariableReferenceExpression> branchDynamicFilters;

            if (projectNode.isPresent()) {
                // Push the project into each union branch, translating its assignments
                ProjectNode project = projectNode.get();
                Assignments.Builder assignments = Assignments.builder();
                Map<VariableReferenceExpression, VariableReferenceExpression> projectVarMapping = new HashMap<>();

                for (Map.Entry<VariableReferenceExpression, RowExpression> entry : project.getAssignments().entrySet()) {
                    RowExpression translatedExpression = RowExpressionVariableInliner.inlineVariables(unionVarMap, entry.getValue());
                    VariableReferenceExpression newVar = context.getVariableAllocator().newVariable(translatedExpression);
                    assignments.put(newVar, translatedExpression);
                    projectVarMapping.put(entry.getKey(), newVar);
                }

                branchSource = new ProjectNode(
                        project.getSourceLocation(),
                        context.getIdAllocator().getNextId(),
                        unionNode.getSources().get(i),
                        assignments.build(),
                        project.getLocality());

                // Map the semi-join source variables through the project variable mapping
                mappedSourceJoinVar = projectVarMapping.get(semiJoinNode.getSourceJoinVariable());
                if (mappedSourceJoinVar == null) {
                    return Result.empty();
                }
                mappedSourceHashVar = semiJoinNode.getSourceHashVariable().map(projectVarMapping::get);
                if (mappedSourceHashVar.isPresent() && mappedSourceHashVar.get() == null) {
                    return Result.empty();
                }

                // Build output-to-input mappings for original union output variables,
                // mapped through the project
                for (VariableReferenceExpression semiJoinOutputVar : semiJoinNode.getOutputVariables()) {
                    if (semiJoinOutputVar.equals(semiJoinNode.getSemiJoinOutput())) {
                        continue; // handled separately below
                    }
                    // This variable comes from the project's output. Map it to the per-branch project output.
                    VariableReferenceExpression branchVar = projectVarMapping.get(semiJoinOutputVar);
                    if (branchVar != null) {
                        outputMappings.put(semiJoinOutputVar, branchVar);
                    }
                }

                // Remap dynamic filter source variables through the project variable mapping
                branchDynamicFilters = remapDynamicFilters(semiJoinNode.getDynamicFilters(), projectVarMapping);
            }
            else {
                branchSource = unionNode.getSources().get(i);

                // Map the semi-join source variables through the union variable mapping
                mappedSourceJoinVar = unionVarMap.get(semiJoinNode.getSourceJoinVariable());
                if (mappedSourceJoinVar == null) {
                    return Result.empty();
                }
                mappedSourceHashVar = semiJoinNode.getSourceHashVariable().map(unionVarMap::get);
                if (mappedSourceHashVar.isPresent() && mappedSourceHashVar.get() == null) {
                    return Result.empty();
                }

                // Build output-to-input mappings for original union output variables
                for (VariableReferenceExpression unionOutputVar : unionNode.getOutputVariables()) {
                    outputMappings.put(unionOutputVar, unionVarMap.get(unionOutputVar));
                }

                // Remap dynamic filter source variables through the union variable mapping
                branchDynamicFilters = remapDynamicFilters(semiJoinNode.getDynamicFilters(), unionVarMap);
            }

            // Allocate new semiJoinOutput variable for each branch
            VariableReferenceExpression newSemiJoinOutput =
                    context.getVariableAllocator().newVariable(semiJoinNode.getSemiJoinOutput());

            // Build new SemiJoinNode for this branch
            SemiJoinNode newSemiJoin = new SemiJoinNode(
                    semiJoinNode.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    branchSource,
                    semiJoinNode.getFilteringSource(),
                    mappedSourceJoinVar,
                    semiJoinNode.getFilteringSourceJoinVariable(),
                    newSemiJoinOutput,
                    mappedSourceHashVar,
                    semiJoinNode.getFilteringSourceHashVariable(),
                    semiJoinNode.getDistributionType(),
                    branchDynamicFilters);

            newSources.add(newSemiJoin);

            // Add the semiJoinOutput mapping
            outputMappings.put(semiJoinNode.getSemiJoinOutput(), newSemiJoinOutput);
        }

        ListMultimap<VariableReferenceExpression, VariableReferenceExpression> mappings = outputMappings.build();

        return Result.ofPlanNode(new UnionNode(
                unionNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                newSources.build(),
                ImmutableList.copyOf(semiJoinNode.getOutputVariables()),
                fromListMultimap(mappings)));
    }

    private static Map<String, VariableReferenceExpression> remapDynamicFilters(
            Map<String, VariableReferenceExpression> dynamicFilters,
            Map<VariableReferenceExpression, VariableReferenceExpression> variableMapping)
    {
        ImmutableMap.Builder<String, VariableReferenceExpression> remapped = ImmutableMap.builder();
        for (Map.Entry<String, VariableReferenceExpression> entry : dynamicFilters.entrySet()) {
            VariableReferenceExpression mappedVar = variableMapping.get(entry.getValue());
            if (mappedVar != null) {
                remapped.put(entry.getKey(), mappedVar);
            }
        }
        return remapped.build();
    }
}
