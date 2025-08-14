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
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.PlannerUtils;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.gatheringExchange;
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
 * 1. Project -> Filter -> TableScan (system) => Project -> Filter -> Exchange -> TableScan
 * 2. Project -> TableScan (system) => Project -> Exchange -> TableScan
 * 3. Filter -> TableScan (system) => Filter -> Exchange -> TableScan
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

        protected boolean shouldAddExchange(Optional<FilterNode> filterNode, Optional<ProjectNode> projectNode)
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

        protected ExchangeNode createGatheringExchange(Context context, TableScanNode tableScanNode)
        {
            return gatheringExchange(
                    context.getIdAllocator().getNextId(),
                    REMOTE_STREAMING,
                    tableScanNode);
        }
    }

    private final class ProjectFilterScanRule
            extends SystemTableFilterRule<ProjectNode>
    {
        private final Capture<ProjectNode> projectCapture = newCapture();
        private final Capture<FilterNode> filterCapture = newCapture();

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project()
                    .capturedAs(projectCapture)
                    .with(source().matching(
                            filter()
                                    .capturedAs(filterCapture)
                                    .with(source().matching(
                                            tableScan()
                                                    .capturedAs(tableScanCapture)
                                                    .matching(PlannerUtils::containsSystemTableScan)))));
        }

        @Override
        public Result apply(ProjectNode node, Captures captures, Context context)
        {
            TableScanNode tableScanNode = captures.get(tableScanCapture);
            ProjectNode projectNode = captures.get(projectCapture);
            FilterNode filterNode = captures.get(filterCapture);

            if (!shouldAddExchange(Optional.of(filterNode), Optional.of(projectNode))) {
                return Result.empty();
            }

            ExchangeNode exchange = createGatheringExchange(context, tableScanNode);

            FilterNode newFilter = new FilterNode(
                    filterNode.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    exchange,
                    filterNode.getPredicate());

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
            extends SystemTableFilterRule<ProjectNode>
    {
        private final Capture<ProjectNode> projectCapture = newCapture();

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project()
                    .capturedAs(projectCapture)
                    .with(source().matching(
                            tableScan()
                                    .capturedAs(tableScanCapture)
                                    .matching(PlannerUtils::containsSystemTableScan)));
        }

        @Override
        public Result apply(ProjectNode node, Captures captures, Context context)
        {
            TableScanNode tableScanNode = captures.get(tableScanCapture);
            ProjectNode projectNode = captures.get(projectCapture);

            if (!shouldAddExchange(Optional.empty(), Optional.of(projectNode))) {
                return Result.empty();
            }

            ExchangeNode exchange = createGatheringExchange(context, tableScanNode);

            ProjectNode newProject = new ProjectNode(
                    projectNode.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    exchange,
                    projectNode.getAssignments(),
                    projectNode.getLocality());

            return Result.ofPlanNode(newProject);
        }
    }

    private final class FilterScanRule
            extends SystemTableFilterRule<FilterNode>
    {
        private final Capture<FilterNode> filterCapture = newCapture();

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return filter()
                    .capturedAs(filterCapture)
                    .with(source().matching(
                            tableScan()
                                    .capturedAs(tableScanCapture)
                                    .matching(PlannerUtils::containsSystemTableScan)));
        }

        @Override
        public Result apply(FilterNode node, Captures captures, Context context)
        {
            TableScanNode tableScanNode = captures.get(tableScanCapture);
            FilterNode filterNode = captures.get(filterCapture);

            if (!shouldAddExchange(Optional.of(filterNode), Optional.empty())) {
                return Result.empty();
            }

            ExchangeNode exchange = createGatheringExchange(context, tableScanNode);

            FilterNode newFilter = new FilterNode(
                    filterNode.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    exchange,
                    filterNode.getPredicate());

            return Result.ofPlanNode(newFilter);
        }
    }
}
