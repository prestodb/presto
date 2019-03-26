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

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class PushdownSubfieldsIntoConnector
        implements Rule<TableScanNode>
{
    private static final Pattern<TableScanNode> PATTERN = tableScan()
            .matching(node -> !node.getRequiredSubfieldPaths().isEmpty());

    private final Metadata metadata;

    public PushdownSubfieldsIntoConnector(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<TableScanNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableScanNode tableScan, Captures captures, Context context)
    {
        Map<ColumnHandle, List<Subfield>> subfields = tableScan.getRequiredSubfieldPaths().entrySet().stream()
                .collect(toImmutableMap(e -> tableScan.getAssignments().get(e.getKey()), Map.Entry::getValue));

        Map<ColumnHandle, ColumnHandle> prunedColumns = metadata.pushdownSubfieldPruning(context.getSession(), tableScan.getTable(), subfields);
        if (prunedColumns.isEmpty()) {
            return Result.empty();
        }

        Map<VariableReferenceExpression, ColumnHandle> newAssignments = tableScan.getAssignments().entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, e -> prunedColumns.getOrDefault(e.getValue(), e.getValue())));

        TableScanNode newTableScanNode = new TableScanNode(
                tableScan.getId(),
                tableScan.getTable(),
                tableScan.getOutputVariables(),
                newAssignments,
                tableScan.getCurrentConstraint(),
                tableScan.getEnforcedConstraint(),
                tableScan.isTemporaryTable(),
                ImmutableMap.of());

        return Result.ofPlanNode(newTableScanNode);
    }
}
