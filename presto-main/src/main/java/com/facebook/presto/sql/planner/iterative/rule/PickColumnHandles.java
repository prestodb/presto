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
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.predicate.FieldSet;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class PickColumnHandles
        implements Rule<TableScanNode>
{
    private final Metadata metadata;
    private static final Pattern<TableScanNode> PATTERN = tableScan();

    public PickColumnHandles(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<TableScanNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableScanNode node, Captures captures, Context context)
    {
        List<Symbol> pruningColumns = node.getAssignments().keySet().stream()
                .filter(symbol -> !symbol.getFields().isEmpty())
                .collect(toImmutableList());

        if (pruningColumns.isEmpty()) {
            return Result.empty();
        }

        List<FieldSet<ColumnHandle>> fieldSets = pruningColumns.stream()
                .map(symbol -> new FieldSet<>(node.getAssignments().get(symbol), symbol.getFields()))
                .collect(toImmutableList());

        List<TableLayoutResult> layouts = metadata.getLayouts(
                context.getSession(),
                node.getTable(),
                new Constraint<>(Optional.of(fieldSets)),
                Optional.of(ImmutableSet.copyOf(node.getAssignments().values())));

        TableLayoutResult layout = layouts.get(0);

        if (!layout.getLayout().getColumns().isPresent() || layout.getLayout().getColumns().get().isEmpty()) {
            return Result.empty();
        }

        List<ColumnHandle> columns = layout.getLayout().getColumns().get();

        Map<Symbol, ColumnHandle> assignments = node.getAssignments().entrySet().stream()
                .map(entry -> {
                    int index = pruningColumns.indexOf(entry.getKey());
                    if (index >= 0) {
                        return Maps.immutableEntry(entry.getKey(), columns.get(index));
                    }
                    return entry;
                }).collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        return Result.ofPlanNode(new TableScanNode(
                node.getId(),
                node.getTable(),
                node.getOutputSymbols(),
                assignments,
                node.getLayout(),
                node.getCurrentConstraint(),
                node.getEnforcedConstraint()));
    }
}
