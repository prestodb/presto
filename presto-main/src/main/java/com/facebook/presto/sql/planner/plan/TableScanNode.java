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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class TableScanNode
        extends PlanNode
{
    private final TableHandle table;
    private final Optional<TableLayoutHandle> tableLayout;
    private final List<Symbol> outputSymbols;
    private final Map<Symbol, ColumnHandle> assignments; // symbol -> column

    // Used during predicate refinement over multiple passes of predicate pushdown
    // TODO: think about how to get rid of this in new planner
    private final TupleDomain<ColumnHandle> currentConstraint;

    // HACK!
    //
    // This field exists for the sole purpose of being able to print the original predicates (from the query) in
    // a human readable way. Predicates that get converted to and from TupleDomains might get more bulky and thus
    // more difficult to read when printed.
    // For example:
    // (ds > '2013-01-01') in the original query could easily become (ds IN ('2013-01-02', '2013-01-03', ...)) after the partitions are generated.
    // To make this work, the originalConstraint should be set exactly once after the first predicate push down and never adjusted after that.
    // In this way, we are always guaranteed to have a readable predicate that provides some kind of upper bound on the constraints.
    private final Expression originalConstraint;

    @JsonCreator
    public TableScanNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("table") TableHandle table,
            @JsonProperty("outputSymbols") List<Symbol> outputs,
            @JsonProperty("assignments") Map<Symbol, ColumnHandle> assignments,
            @JsonProperty("layout") Optional<TableLayoutHandle> tableLayout,
            @JsonProperty("currentConstraint") TupleDomain<ColumnHandle> currentConstraint,
            @JsonProperty("originalConstraint") @Nullable Expression originalConstraint)
    {
        super(id);
        requireNonNull(table, "table is null");
        requireNonNull(outputs, "outputs is null");
        requireNonNull(assignments, "assignments is null");
        checkArgument(assignments.keySet().containsAll(outputs), "assignments does not cover all of outputs");
        requireNonNull(tableLayout, "tableLayout is null");
        requireNonNull(currentConstraint, "currentConstraint is null");

        this.table = table;
        this.outputSymbols = ImmutableList.copyOf(outputs);
        this.assignments = ImmutableMap.copyOf(assignments);
        this.originalConstraint = originalConstraint;
        this.tableLayout = tableLayout;
        this.currentConstraint = currentConstraint;
    }

    @JsonProperty("table")
    public TableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public Optional<TableLayoutHandle> getLayout()
    {
        return tableLayout;
    }

    @Override
    @JsonProperty("outputSymbols")
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    @JsonProperty("assignments")
    public Map<Symbol, ColumnHandle> getAssignments()
    {
        return assignments;
    }

    @Nullable
    @JsonProperty("originalConstraint")
    public Expression getOriginalConstraint()
    {
        return originalConstraint;
    }

    @JsonProperty("currentConstraint")
    public TupleDomain<ColumnHandle> getCurrentConstraint()
    {
        return currentConstraint;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitTableScan(this, context);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("tableLayout", tableLayout)
                .add("outputSymbols", outputSymbols)
                .add("assignments", assignments)
                .add("currentConstraint", currentConstraint)
                .add("originalConstraint", originalConstraint)
                .toString();
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.isEmpty(), "newChildren is not empty");
        return this;
    }
}
