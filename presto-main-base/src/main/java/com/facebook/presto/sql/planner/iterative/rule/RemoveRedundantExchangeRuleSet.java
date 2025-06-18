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
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.PlannerUtils;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.PlannerUtils.containsSystemTableScan;
import static com.facebook.presto.sql.planner.plan.Patterns.exchange;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static com.facebook.presto.sql.relational.RowExpressionUtils.containsNonCoordinatorEligibleCallExpression;
import static java.util.Objects.requireNonNull;

/**
 * RuleSet for removing redundant exchanges that were added by SystemTableFilterRuleSet.
 *
 * For system tables with CPP functions, SystemTableFilterRuleSet adds an exchange
 * to ensure CPP functions run on workers. However, if there's already an outer exchange in the plan,
 * we end up with redundant exchanges.
 *
 * Patterns handled:
 * 1. Exchange -> Filter -> Exchange -> TableScan => Filter -> Exchange -> TableScan
 * 2. Exchange -> Project -> Filter -> Exchange -> TableScan => Project -> Filter -> Exchange -> TableScan
 * 3. Exchange -> Project -> Exchange -> TableScan => Project -> Exchange -> TableScan
 */
public class RemoveRedundantExchangeRuleSet
{
    private final FunctionAndTypeManager functionAndTypeManager;

    public RemoveRedundantExchangeRuleSet(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                new ExchangeFilterExchangeScanRule(),
                new ExchangeProjectFilterExchangeScanRule(),
                new ExchangeProjectExchangeScanRule());
    }

    private abstract class RedundantExchangeRemovalRule
            implements Rule<ExchangeNode>
    {
        protected boolean shouldKeepOuterExchange(
                TableScanNode tableScanNode,
                Optional<FilterNode> filterNode,
                Optional<ProjectNode> projectNode)
        {
            // Only process system tables
            if (!containsSystemTableScan(tableScanNode)) {
                return true;
            }

            // Check if there are CPP functions that require worker execution
            boolean hasNonCoordinatorEligibleFunctions = false;

            if (filterNode.isPresent()) {
                hasNonCoordinatorEligibleFunctions |= containsNonCoordinatorEligibleCallExpression(
                        functionAndTypeManager, filterNode.get().getPredicate());
            }

            if (projectNode.isPresent()) {
                hasNonCoordinatorEligibleFunctions |= projectNode.get().getAssignments().getExpressions().stream()
                        .anyMatch(expression -> containsNonCoordinatorEligibleCallExpression(
                                functionAndTypeManager, expression));
            }

            // Remove the outer exchange when:
            // 1. It's a system table AND
            // 2. There are CPP functions (the inner exchange will ensure worker execution)
            return !hasNonCoordinatorEligibleFunctions;
        }

        protected boolean isSingleSourceExchange(ExchangeNode exchange)
        {
            return exchange.getSources().size() == 1;
        }
    }

    private final class ExchangeFilterExchangeScanRule
            extends RedundantExchangeRemovalRule
    {
        private final Capture<ExchangeNode> outerExchangeCapture = newCapture();
        private final Capture<FilterNode> filterCapture = newCapture();
        private final Capture<ExchangeNode> innerExchangeCapture = newCapture();
        private final Capture<TableScanNode> tableScanCapture = newCapture();

        @Override
        public Pattern<ExchangeNode> getPattern()
        {
            return exchange()
                    .capturedAs(outerExchangeCapture)
                    .matching(this::isSingleSourceExchange)
                    .with(source().matching(
                            filter()
                                    .capturedAs(filterCapture)
                                    .with(source().matching(
                                            exchange()
                                                    .capturedAs(innerExchangeCapture)
                                                    .matching(this::isSingleSourceExchange)
                                                    .with(source().matching(
                                                            tableScan()
                                                                    .capturedAs(tableScanCapture)))))));
        }

        @Override
        public Result apply(ExchangeNode node, Captures captures, Context context)
        {
            TableScanNode tableScanNode = captures.get(tableScanCapture);
            FilterNode filterNode = captures.get(filterCapture);

            if (shouldKeepOuterExchange(tableScanNode, Optional.of(filterNode), Optional.empty())) {
                return Result.empty();
            }

            // Return the filter node directly, removing the outer exchange
            return Result.ofPlanNode(filterNode);
        }
    }

    private final class ExchangeProjectFilterExchangeScanRule
            extends RedundantExchangeRemovalRule
    {
        private final Capture<ExchangeNode> outerExchangeCapture = newCapture();
        private final Capture<ProjectNode> projectCapture = newCapture();
        private final Capture<FilterNode> filterCapture = newCapture();
        private final Capture<ExchangeNode> innerExchangeCapture = newCapture();
        private final Capture<TableScanNode> tableScanCapture = newCapture();

        @Override
        public Pattern<ExchangeNode> getPattern()
        {
            return exchange()
                    .capturedAs(outerExchangeCapture)
                    .matching(this::isSingleSourceExchange)
                    .with(source().matching(
                            project()
                                    .capturedAs(projectCapture)
                                    .with(source().matching(
                                            filter()
                                                    .capturedAs(filterCapture)
                                                    .with(source().matching(
                                                            exchange()
                                                                    .capturedAs(innerExchangeCapture)
                                                                    .matching(this::isSingleSourceExchange)
                                                                    .with(source().matching(
                                                                            tableScan()
                                                                                    .capturedAs(tableScanCapture)))))))));
        }

        @Override
        public Result apply(ExchangeNode node, Captures captures, Context context)
        {
            TableScanNode tableScanNode = captures.get(tableScanCapture);
            FilterNode filterNode = captures.get(filterCapture);
            ProjectNode projectNode = captures.get(projectCapture);

            if (shouldKeepOuterExchange(tableScanNode, Optional.of(filterNode), Optional.of(projectNode))) {
                return Result.empty();
            }

            // Return the project node directly, removing the outer exchange
            return Result.ofPlanNode(projectNode);
        }
    }

    private final class ExchangeProjectExchangeScanRule
            extends RedundantExchangeRemovalRule
    {
        private final Capture<ExchangeNode> outerExchangeCapture = newCapture();
        private final Capture<ProjectNode> projectCapture = newCapture();
        private final Capture<ExchangeNode> innerExchangeCapture = newCapture();
        private final Capture<TableScanNode> tableScanCapture = newCapture();

        @Override
        public Pattern<ExchangeNode> getPattern()
        {
            return exchange()
                    .capturedAs(outerExchangeCapture)
                    .matching(this::isSingleSourceExchange)
                    .with(source().matching(
                            project()
                                    .capturedAs(projectCapture)
                                    .with(source().matching(
                                            exchange()
                                                    .capturedAs(innerExchangeCapture)
                                                    .matching(this::isSingleSourceExchange)
                                                    .with(source().matching(
                                                            tableScan()
                                                                    .capturedAs(tableScanCapture)))))));
        }

        @Override
        public Result apply(ExchangeNode node, Captures captures, Context context)
        {
            TableScanNode tableScanNode = captures.get(tableScanCapture);
            ProjectNode projectNode = captures.get(projectCapture);

            if (shouldKeepOuterExchange(tableScanNode, Optional.empty(), Optional.of(projectNode))) {
                return Result.empty();
            }

            // Return the project node directly, removing the outer exchange
            return Result.ofPlanNode(projectNode);
        }
    }
}
