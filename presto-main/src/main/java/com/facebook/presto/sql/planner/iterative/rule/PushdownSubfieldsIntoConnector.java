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
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.scalar.FilterBySubscriptPathsFunction;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SubfieldPath;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isPushdownSubfields;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class PushdownSubfieldsIntoConnector
{
    private final Metadata metadata;

    public PushdownSubfieldsIntoConnector(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                new PushdownSubfieldsIntoConnectorWithoutFilter(metadata),
                new PushdownSubfieldsIntoConnectorWithFilter(metadata));
    }

    private static final class PushdownSubfieldsIntoConnectorWithoutFilter
            extends AbstractPushdownSubfieldsIntoConnector
    {
        private static final Capture<TableScanNode> TABLE_SCAN = newCapture();

        private static final Pattern<ProjectNode> PATTERN = project()
                .with(source().matching(tableScan().capturedAs(TABLE_SCAN)));

        private PushdownSubfieldsIntoConnectorWithoutFilter(Metadata metadata)
        {
            super(metadata);
        }

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(ProjectNode projectNode, Captures captures, Context context)
        {
            return apply(projectNode, Optional.empty(), captures.get(TABLE_SCAN), context);
        }
    }

    private static final class PushdownSubfieldsIntoConnectorWithFilter
            extends AbstractPushdownSubfieldsIntoConnector
    {
        private static final Capture<FilterNode> FILTER = newCapture();
        private static final Capture<TableScanNode> TABLE_SCAN = newCapture();

        private static final Pattern<ProjectNode> PATTERN = project()
                .with(source().matching(filter().capturedAs(FILTER)
                        .with(source().matching(tableScan().capturedAs(TABLE_SCAN)))));

        private PushdownSubfieldsIntoConnectorWithFilter(Metadata metadata)
        {
            super(metadata);
        }

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(ProjectNode projectNode, Captures captures, Context context)
        {
            return apply(projectNode, Optional.of(captures.get(FILTER)), captures.get(TABLE_SCAN), context);
        }
    }

    private abstract static class AbstractPushdownSubfieldsIntoConnector
            implements Rule<ProjectNode>
    {
        private static final QualifiedName FILTER_BY_SUBSCRIPT_PATHS = QualifiedName.of(FilterBySubscriptPathsFunction.FILTER_BY_SUBSCRIPT_PATHS);

        private final Metadata metadata;

        private AbstractPushdownSubfieldsIntoConnector(Metadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return isPushdownSubfields(session);
        }

        protected Result apply(ProjectNode project, Optional<FilterNode> filter, TableScanNode tableScan, Context context)
        {
            Map<Symbol, Symbol> symbolsToPrune = project.getAssignments().getMap().entrySet().stream()
                    .filter(e -> e.getValue() instanceof FunctionCall)
                    .filter(e -> ((FunctionCall) e.getValue()).getName().equals(FILTER_BY_SUBSCRIPT_PATHS))
                    .collect(toImmutableMap(
                            Map.Entry::getKey,
                            e -> Symbol.from(((FunctionCall) e.getValue()).getArguments().get(0))));

            Map<ColumnHandle, List<SubfieldPath>> subfields = project.getAssignments().getExpressions().stream()
                    .filter(FunctionCall.class::isInstance)
                    .map(FunctionCall.class::cast)
                    .filter(e -> e.getName().equals(FILTER_BY_SUBSCRIPT_PATHS))
                    .collect(toImmutableMap(
                            e -> tableScan.getAssignments().get(Symbol.from(e.getArguments().get(0))),
                            e -> ((ArrayConstructor) e.getArguments().get(1)).getValues().stream()
                                    .map(path -> new SubfieldPath(((StringLiteral) path).getValue()))
                                    .collect(toImmutableList())));
            if (subfields.isEmpty()) {
                return Result.empty();
            }

            Map<ColumnHandle, ColumnHandle> prunedColumns = metadata.pushdownSubfieldPruning(context.getSession(), tableScan.getTable(), subfields);
            if (prunedColumns.isEmpty()) {
                return Result.empty();
            }

            Assignments.Builder newAssignments = Assignments.builder();
            for (Map.Entry<Symbol, Expression> entry : project.getAssignments().entrySet()) {
                Symbol projectSymbol = entry.getKey();
                Symbol tableScanSymbol = symbolsToPrune.get(projectSymbol);
                if (tableScanSymbol != null && prunedColumns.containsKey(tableScan.getAssignments().get(tableScanSymbol))) {
                    newAssignments.put(projectSymbol, tableScanSymbol.toSymbolReference());
                }
                else {
                    newAssignments.put(entry);
                }
            }

            TableScanNode newTableScanNode = new TableScanNode(
                    tableScan.getId(),
                    tableScan.getTable(),
                    tableScan.getOutputSymbols(),
                    tableScan.getAssignments().entrySet().stream()
                            .collect(toImmutableMap(Map.Entry::getKey, e -> prunedColumns.getOrDefault(e.getValue(), e.getValue()))),
                    tableScan.getCurrentConstraint(),
                    tableScan.getEnforcedConstraint());

            if (filter.isPresent()) {
                return Result.ofPlanNode(new ProjectNode(
                        project.getId(),
                        new FilterNode(filter.get().getId(), newTableScanNode, filter.get().getPredicate()),
                        newAssignments.build()));
            }

            return Result.ofPlanNode(new ProjectNode(project.getId(), newTableScanNode, newAssignments.build()));
        }
    }
}
