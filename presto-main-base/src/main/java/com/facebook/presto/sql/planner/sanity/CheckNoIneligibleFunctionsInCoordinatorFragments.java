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
package com.facebook.presto.sql.planner.sanity;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.plan.ExchangeNode;

import static com.facebook.presto.sql.planner.PlannerUtils.containsSystemTableScan;
import static com.facebook.presto.sql.relational.RowExpressionUtils.containsNonCoordinatorEligibleCallExpression;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Validates that there are no filter or projection nodes containing non-Java functions
 * (which must be evaluated on native nodes) within the same fragment as a system table scan
 * (which must be evaluated on the coordinator).
 */
public class CheckNoIneligibleFunctionsInCoordinatorFragments
        implements PlanChecker.Checker
{
    @Override
    public void validate(PlanNode planNode, Session session, Metadata metadata, WarningCollector warningCollector)
    {
        FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
        // Validate each fragment independently
        validateFragment(planNode, functionAndTypeManager);
    }

    private void validateFragment(PlanNode root, FunctionAndTypeManager functionAndTypeManager)
    {
        // First, collect information about this fragment
        FragmentValidator validator = new FragmentValidator(functionAndTypeManager);
        root.accept(validator, null);

        // Check if this fragment violates the constraint
        checkState(
                !(validator.hasSystemTableScan() && validator.hasNonCoordinatorEligibleFunction()),
                "Fragment contains both system table scan and non-Java functions. " +
                "System table scans must execute on the coordinator while non-Java functions must execute on native nodes. " +
                "These operations must be in separate fragments separated by an exchange.");

        // Recursively validate child fragments
        ChildFragmentVisitor childVisitor = new ChildFragmentVisitor(functionAndTypeManager);
        root.accept(childVisitor, null);
    }

    /**
     * Visits nodes within a single fragment to collect information about
     * system table scans and non-coordinator-eligible functions.
     * Stops at exchange boundaries.
     */
    private static class FragmentValidator
            extends SimplePlanVisitor<Void>
    {
        private final FunctionAndTypeManager functionAndTypeManager;
        private boolean hasSystemTableScan;
        private boolean hasNonCoordinatorEligibleFunction;

        public FragmentValidator(FunctionAndTypeManager functionAndTypeManager)
        {
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        }

        public boolean hasSystemTableScan()
        {
            return hasSystemTableScan;
        }

        public boolean hasNonCoordinatorEligibleFunction()
        {
            return hasNonCoordinatorEligibleFunction;
        }

        @Override
        public Void visitExchange(ExchangeNode node, Void context)
        {
            // Don't traverse into exchange sources - they are different fragments
            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Void context)
        {
            if (containsSystemTableScan(node)) {
                hasSystemTableScan = true;
            }
            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Void context)
        {
            RowExpression predicate = node.getPredicate();
            if (containsNonCoordinatorEligibleCallExpression(functionAndTypeManager, predicate)) {
                hasNonCoordinatorEligibleFunction = true;
            }
            return visitPlan(node, context);
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            boolean hasIneligibleProjections = node.getAssignments().getExpressions().stream()
                    .anyMatch(expression -> containsNonCoordinatorEligibleCallExpression(functionAndTypeManager, expression));

            if (hasIneligibleProjections) {
                hasNonCoordinatorEligibleFunction = true;
            }
            return visitPlan(node, context);
        }

        @Override
        public Void visitPlan(PlanNode node, Void context)
        {
            for (PlanNode source : node.getSources()) {
                source.accept(this, context);
            }
            return null;
        }
    }

    /**
     * Visits nodes to find and validate child fragments (those below exchanges).
     */
    private class ChildFragmentVisitor
            extends SimplePlanVisitor<Void>
    {
        private final FunctionAndTypeManager functionAndTypeManager;

        public ChildFragmentVisitor(FunctionAndTypeManager functionAndTypeManager)
        {
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        }

        @Override
        public Void visitExchange(ExchangeNode node, Void context)
        {
            // Each source of an exchange is a separate fragment
            for (PlanNode source : node.getSources()) {
                validateFragment(source, functionAndTypeManager);
            }
            return null;
        }

        @Override
        public Void visitPlan(PlanNode node, Void context)
        {
            for (PlanNode source : node.getSources()) {
                source.accept(this, context);
            }
            return null;
        }
    }
}
