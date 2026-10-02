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
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.ValuesNode;
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
import static com.facebook.presto.sql.planner.iterative.Plans.resolveGroupReferences;
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
 * <p>
 * The filtering source is duplicated into every union branch, so it is copied with freshly allocated
 * plan node ids. The rule does not fire when that subtree cannot be copied.
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

        // Every branch gets its own semi join, and therefore its own copy of the filtering source. The same
        // subtree cannot be shared by the branches: the plan is a tree, so sharing it leaves the plan with
        // duplicated plan node ids, which the plan checker rejects. Materialize the subtree behind the group
        // reference so it can be copied node by node with freshly allocated ids.
        PlanNode filteringSource = resolveGroupReferences(semiJoinNode.getFilteringSource(), context.getLookup());

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

            // Copy the filtering source for this branch. The copy keeps the original variables, so the
            // filtering source join and hash variables stay valid, but every node in it gets a new id.
            Optional<PlanNode> branchFilteringSource = copyWithNewPlanNodeIds(filteringSource, context.getIdAllocator());
            if (!branchFilteringSource.isPresent()) {
                return Result.empty();
            }

            // Build new SemiJoinNode for this branch
            SemiJoinNode newSemiJoin = new SemiJoinNode(
                    semiJoinNode.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    branchSource,
                    branchFilteringSource.get(),
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

    /**
     * Rebuilds the subtree with a freshly allocated plan node id for every node. Variables are left
     * untouched, so the copy produces exactly the same output variables as the original.
     * <p>
     * Returns {@link Optional#empty()} for a subtree containing a node type this method cannot rebuild,
     * in which case the rule does not fire.
     */
    private static Optional<PlanNode> copyWithNewPlanNodeIds(PlanNode node, PlanNodeIdAllocator idAllocator)
    {
        if (node instanceof TableScanNode) {
            TableScanNode tableScanNode = (TableScanNode) node;
            return Optional.of(new TableScanNode(
                    tableScanNode.getSourceLocation(),
                    idAllocator.getNextId(),
                    tableScanNode.getStatsEquivalentPlanNode(),
                    tableScanNode.getTable(),
                    tableScanNode.getOutputVariables(),
                    tableScanNode.getAssignments(),
                    tableScanNode.getTableConstraints(),
                    tableScanNode.getCurrentConstraint(),
                    tableScanNode.getEnforcedConstraint(),
                    tableScanNode.getCteMaterializationInfo()));
        }

        if (node instanceof ValuesNode) {
            ValuesNode valuesNode = (ValuesNode) node;
            return Optional.of(new ValuesNode(
                    valuesNode.getSourceLocation(),
                    idAllocator.getNextId(),
                    valuesNode.getStatsEquivalentPlanNode(),
                    valuesNode.getOutputVariables(),
                    valuesNode.getRows(),
                    valuesNode.getValuesNodeLabel()));
        }

        if (node instanceof FilterNode) {
            FilterNode filterNode = (FilterNode) node;
            return copyWithNewPlanNodeIds(filterNode.getSource(), idAllocator)
                    .map(newSource -> new FilterNode(
                            filterNode.getSourceLocation(),
                            idAllocator.getNextId(),
                            filterNode.getStatsEquivalentPlanNode(),
                            newSource,
                            filterNode.getPredicate()));
        }

        if (node instanceof ProjectNode) {
            ProjectNode project = (ProjectNode) node;
            return copyWithNewPlanNodeIds(project.getSource(), idAllocator)
                    .map(newSource -> new ProjectNode(
                            project.getSourceLocation(),
                            idAllocator.getNextId(),
                            project.getStatsEquivalentPlanNode(),
                            newSource,
                            project.getAssignments(),
                            project.getLocality()));
        }

        if (node instanceof AggregationNode) {
            AggregationNode aggregationNode = (AggregationNode) node;
            return copyWithNewPlanNodeIds(aggregationNode.getSource(), idAllocator)
                    .map(newSource -> new AggregationNode(
                            aggregationNode.getSourceLocation(),
                            idAllocator.getNextId(),
                            aggregationNode.getStatsEquivalentPlanNode(),
                            newSource,
                            aggregationNode.getAggregations(),
                            aggregationNode.getGroupingSets(),
                            aggregationNode.getPreGroupedVariables(),
                            aggregationNode.getStep(),
                            aggregationNode.getHashVariable(),
                            aggregationNode.getGroupIdVariable(),
                            aggregationNode.getAggregationId()));
        }

        if (node instanceof UnionNode) {
            UnionNode unionNode = (UnionNode) node;
            ImmutableList.Builder<PlanNode> newSources = ImmutableList.builder();
            for (PlanNode source : unionNode.getSources()) {
                Optional<PlanNode> newSource = copyWithNewPlanNodeIds(source, idAllocator);
                if (!newSource.isPresent()) {
                    return Optional.empty();
                }
                newSources.add(newSource.get());
            }
            return Optional.of(new UnionNode(
                    unionNode.getSourceLocation(),
                    idAllocator.getNextId(),
                    unionNode.getStatsEquivalentPlanNode(),
                    newSources.build(),
                    unionNode.getOutputVariables(),
                    unionNode.getVariableMapping()));
        }

        return Optional.empty();
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
