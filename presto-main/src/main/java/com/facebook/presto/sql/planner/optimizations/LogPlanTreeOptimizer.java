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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.google.common.base.Strings;

import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;

public class LogPlanTreeOptimizer
        implements PlanOptimizer
{
    private static final Logger log = Logger.get(LogPlanTreeOptimizer.class);
    private final String stateMarker;

    public LogPlanTreeOptimizer(String stateMarker)
    {
        this.stateMarker = stateMarker;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        log.info("------ %s", stateMarker);
        MinimalTreePrinter minimalTreePrinter = new MinimalTreePrinter(Lookup.noLookup());
        plan.accept(minimalTreePrinter, 0);
        log.info("Plan :%n%s", minimalTreePrinter.result());
        return plan;
    }

    public static class MinimalTreePrinter
            extends InternalPlanVisitor<Void, Integer>
    {
        private final StringBuilder result = new StringBuilder();
        private final Lookup lookup;

        public MinimalTreePrinter(Lookup lookup)
        {
            this.lookup = lookup;
        }

        public String result()
        {
            return result.toString();
        }

        @Override
        public Void visitPlan(PlanNode node, Integer indent)
        {
            PlanNode resolved = node;
            try {
                resolved = lookup.resolve(node);
            }
            catch (Exception ex) {
                // Eat this exception
            }
            for (PlanNode source : resolved.getSources()) {
                source.accept(this, indent);
            }
            return null;
        }

        @Override
        public Void visitGroupReference(GroupReference node, Integer context)
        {
            lookup.resolve(node).accept(this, context);
            return null;
        }

        @Override
        public Void visitProject(ProjectNode node, Integer indent)
        {
            output(indent, "project, Assignments (%s), Outputs (%s):", node.getAssignments().getMap(), node.getOutputVariables());

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitFilter(FilterNode node, Integer indent)
        {
            output(indent, "filter (predicate = %s)", node.getPredicate());

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitJoin(JoinNode node, Integer indent)
        {
            output(indent, "join (%s), Equi-join condition(%s), Filter (%s), Outputs(%s):", node.getType(), node.getCriteria(), node.getFilter(), node.getOutputVariables());

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitAggregation(AggregationNode node, Integer indent)
        {
            output(
                    indent,
                    "%s aggregation over (%s)",
                    node.getStep().name().toLowerCase(ENGLISH),
                    node.getGroupingKeys().stream()
                            .map(Object::toString)
                            .sorted()
                            .collect(joining(", ")));

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Integer indent)
        {
            ConnectorTableHandle connectorTableHandle = node.getTable().getConnectorHandle();
            output(indent, "scan (%s) Constraints [Current (%s), Enforced(%s)]",
                    connectorTableHandle.toString(),
                    node.getCurrentConstraint().getDomains(),
                    node.getEnforcedConstraint().getDomains());
            return null;
        }

        @Override
        public Void visitSemiJoin(final SemiJoinNode node, Integer indent)
        {
            output(indent, "semijoin");
            return visitPlan(node, indent + 1);
        }

        private void output(int indent, String message, Object... args)
        {
            String formattedMessage = format(message, args);
            result.append(format("%s%s\n", Strings.repeat("    ", indent), formattedMessage));
        }
    }
}
