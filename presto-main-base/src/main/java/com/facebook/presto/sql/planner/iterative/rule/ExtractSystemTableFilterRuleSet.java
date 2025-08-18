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

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlannerUtils;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.plan.Patterns.exchange;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static com.facebook.presto.sql.relational.RowExpressionUtils.containsNonCoordinatorEligibleCallExpression;
import static java.util.Objects.requireNonNull;

/**
 * RuleSet for extracting system table filters when they contain non-coordinator-eligible functions (e.g., CPP functions).
 * This ensures that system table scans happen on the coordinator while CPP functions execute on workers.
 *
 * Patterns handled:
 * 1. Exchange -> Project -> Filter -> TableScan (system) => Project -> Filter -> Exchange -> TableScan
 * 2. Exchange -> Project -> TableScan (system) => Project -> Exchange -> TableScan
 * 3. Exchange -> Filter -> TableScan (system) => Filter -> Exchange -> TableScan
 */
public class ExtractSystemTableFilterRuleSet
{
    private final FunctionAndTypeManager functionAndTypeManager;

    public ExtractSystemTableFilterRuleSet(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                new ProjectFilterScanRule(),
                new ProjectScanRule(),
                new FilterScanRule());
    }

    private abstract class SystemTableFilterRule<T extends PlanNode>
            implements Rule<T>
    {
        protected final Capture<TableScanNode> tableScanCapture = newCapture();

        protected boolean containsFunctionsIneligibleOnCoordinator(Optional<FilterNode> filterNode, Optional<ProjectNode> projectNode)
        {
            boolean hasIneligiblePredicates = filterNode
                    .map(filter -> containsNonCoordinatorEligibleCallExpression(functionAndTypeManager, filter.getPredicate()))
                    .orElse(false);

            boolean hasIneligibleProjections = projectNode
                    .map(project -> project.getAssignments().getExpressions().stream()
                            .anyMatch(expression -> containsNonCoordinatorEligibleCallExpression(functionAndTypeManager, expression)))
                    .orElse(false);

            return hasIneligiblePredicates || hasIneligibleProjections;
        }
    }

    private final class ProjectFilterScanRule
            extends SystemTableFilterRule<ExchangeNode>
    {
        private final Capture<ExchangeNode> exchangeCapture = newCapture();
        private final Capture<ProjectNode> projectCapture = newCapture();
        private final Capture<FilterNode> filterCapture = newCapture();

        @Override
        public Pattern<ExchangeNode> getPattern()
        {
            return exchange()
                    .capturedAs(exchangeCapture)
                    .with(source().matching(
                            project()
                                    .capturedAs(projectCapture)
                                    .with(source().matching(
                                            filter()
                                                    .capturedAs(filterCapture)
                                                    .with(source().matching(
                                                            tableScan()
                                                                    .capturedAs(tableScanCapture)
                                                                    .matching(PlannerUtils::containsSystemTableScan)))))));
        }

        @Override
        public Result apply(ExchangeNode node, Captures captures, Context context)
        {
            TableScanNode tableScanNode = captures.get(tableScanCapture);
            ExchangeNode exchangeNode = captures.get(exchangeCapture);
            ProjectNode projectNode = captures.get(projectCapture);
            FilterNode filterNode = captures.get(filterCapture);

            if (!containsFunctionsIneligibleOnCoordinator(Optional.of(filterNode), Optional.of(projectNode))) {
                return Result.empty();
            }

            // The exchange's output variables must match what the filter expects
            // Since the filter was originally between project and table scan, it expects
            // the table scan's output variables
            PartitioningScheme newPartitioningScheme = new PartitioningScheme(
                    exchangeNode.getPartitioningScheme().getPartitioning(),
                    tableScanNode.getOutputVariables(),
                    exchangeNode.getPartitioningScheme().getHashColumn(),
                    exchangeNode.getPartitioningScheme().isScaleWriters(),
                    exchangeNode.getPartitioningScheme().isReplicateNullsAndAny(),
                    exchangeNode.getPartitioningScheme().getEncoding(),
                    exchangeNode.getPartitioningScheme().getBucketToPartition());

            // Create new exchange with table scan as source
            ExchangeNode newExchange = new ExchangeNode(
                    exchangeNode.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    exchangeNode.getType(),
                    exchangeNode.getScope(),
                    newPartitioningScheme,
                    ImmutableList.of(tableScanNode),
                    ImmutableList.of(tableScanNode.getOutputVariables()),
                    exchangeNode.isEnsureSourceOrdering(),
                    exchangeNode.getOrderingScheme());

            // Recreate filter with exchange as source
            FilterNode newFilter = new FilterNode(
                    filterNode.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    newExchange,
                    filterNode.getPredicate());

            // Recreate project with filter as source
            ProjectNode newProject = new ProjectNode(
                    projectNode.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    newFilter,
                    projectNode.getAssignments(),
                    projectNode.getLocality());

            return Result.ofPlanNode(newProject);
        }
    }

    private final class ProjectScanRule
            extends SystemTableFilterRule<ExchangeNode>
    {
        private final Capture<ExchangeNode> exchangeCapture = newCapture();
        private final Capture<ProjectNode> projectCapture = newCapture();

        @Override
        public Pattern<ExchangeNode> getPattern()
        {
            return exchange()
                    .capturedAs(exchangeCapture)
                    .with(source().matching(
                            project()
                                    .capturedAs(projectCapture)
                                    .with(source().matching(
                                            tableScan()
                                                    .capturedAs(tableScanCapture)
                                                    .matching(PlannerUtils::containsSystemTableScan)))));
        }

        @Override
        public Result apply(ExchangeNode node, Captures captures, Context context)
        {
            TableScanNode tableScanNode = captures.get(tableScanCapture);
            ExchangeNode exchangeNode = captures.get(exchangeCapture);
            ProjectNode projectNode = captures.get(projectCapture);

            if (!containsFunctionsIneligibleOnCoordinator(Optional.empty(), Optional.of(projectNode))) {
                return Result.empty();
            }

            // Update partitioning scheme to match table scan outputs
            PartitioningScheme newPartitioningScheme = new PartitioningScheme(
                    exchangeNode.getPartitioningScheme().getPartitioning(),
                    tableScanNode.getOutputVariables(),
                    exchangeNode.getPartitioningScheme().getHashColumn(),
                    exchangeNode.getPartitioningScheme().isScaleWriters(),
                    exchangeNode.getPartitioningScheme().isReplicateNullsAndAny(),
                    exchangeNode.getPartitioningScheme().getEncoding(),
                    exchangeNode.getPartitioningScheme().getBucketToPartition());

            // Create new exchange with table scan as source
            ExchangeNode newExchange = new ExchangeNode(
                    exchangeNode.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    exchangeNode.getType(),
                    exchangeNode.getScope(),
                    newPartitioningScheme,
                    ImmutableList.of(tableScanNode),
                    ImmutableList.of(tableScanNode.getOutputVariables()),
                    exchangeNode.isEnsureSourceOrdering(),
                    exchangeNode.getOrderingScheme());

            // Recreate project with exchange as source
            ProjectNode newProject = new ProjectNode(
                    projectNode.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    newExchange,
                    projectNode.getAssignments(),
                    projectNode.getLocality());

            return Result.ofPlanNode(newProject);
        }
    }

    private final class FilterScanRule
            extends SystemTableFilterRule<ExchangeNode>
    {
        private final Capture<ExchangeNode> exchangeCapture = newCapture();
        private final Capture<FilterNode> filterCapture = newCapture();

        @Override
        public Pattern<ExchangeNode> getPattern()
        {
            return exchange()
                    .capturedAs(exchangeCapture)
                    .with(source().matching(
                            filter()
                                    .capturedAs(filterCapture)
                                    .with(source().matching(
                                            tableScan()
                                                    .capturedAs(tableScanCapture)
                                                    .matching(PlannerUtils::containsSystemTableScan)))));
        }

        @Override
        public Result apply(ExchangeNode node, Captures captures, Context context)
        {
            TableScanNode tableScanNode = captures.get(tableScanCapture);
            ExchangeNode exchangeNode = captures.get(exchangeCapture);
            FilterNode filterNode = captures.get(filterCapture);

            if (!containsFunctionsIneligibleOnCoordinator(Optional.of(filterNode), Optional.empty())) {
                return Result.empty();
            }

            // Update partitioning scheme to match table scan outputs
            PartitioningScheme newPartitioningScheme = new PartitioningScheme(
                    exchangeNode.getPartitioningScheme().getPartitioning(),
                    tableScanNode.getOutputVariables(),
                    exchangeNode.getPartitioningScheme().getHashColumn(),
                    exchangeNode.getPartitioningScheme().isScaleWriters(),
                    exchangeNode.getPartitioningScheme().isReplicateNullsAndAny(),
                    exchangeNode.getPartitioningScheme().getEncoding(),
                    exchangeNode.getPartitioningScheme().getBucketToPartition());

            // Create new exchange with table scan as source
            ExchangeNode newExchange = new ExchangeNode(
                    exchangeNode.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    exchangeNode.getType(),
                    exchangeNode.getScope(),
                    newPartitioningScheme,
                    ImmutableList.of(tableScanNode),
                    ImmutableList.of(tableScanNode.getOutputVariables()),
                    exchangeNode.isEnsureSourceOrdering(),
                    exchangeNode.getOrderingScheme());

            // Recreate filter with exchange as source
            FilterNode newFilter = new FilterNode(
                    filterNode.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    newExchange,
                    filterNode.getPredicate());

            // Check if the original exchange's output variables match the filter's output
            // If not, add a project node to align them
            if (!exchangeNode.getOutputVariables().equals(newFilter.getOutputVariables())) {
                Assignments.Builder assignments = Assignments.builder();
                for (VariableReferenceExpression variable : exchangeNode.getOutputVariables()) {
                    assignments.put(variable, variable);
                }

                ProjectNode projectNode = new ProjectNode(
                        exchangeNode.getSourceLocation(),
                        context.getIdAllocator().getNextId(),
                        newFilter,
                        assignments.build(),
                        ProjectNode.Locality.LOCAL);

                return Result.ofPlanNode(projectNode);
            }

            return Result.ofPlanNode(newFilter);
        }
    }
}
