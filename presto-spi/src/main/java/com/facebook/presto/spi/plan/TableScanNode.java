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
package com.facebook.presto.spi.plan;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

@Immutable
public final class TableScanNode
        extends PlanNode
{
    private final TableHandle table;
    private final Map<VariableReferenceExpression, ColumnHandle> assignments;
    private final List<VariableReferenceExpression> outputVariables;

    // Used during predicate refinement over multiple passes of predicate pushdown
    // TODO: think about how to get rid of this in new planner
    // TODO: these two fields will not be effective if they are created by connectors until we have refactored PickTableLayout
    private final TupleDomain<ColumnHandle> currentConstraint;
    private final TupleDomain<ColumnHandle> enforcedConstraint;
    private final List<TableConstraint<ColumnHandle>> tableConstraints;

    /**
     * This constructor is for JSON deserialization only.  Do not use!
     */
    @JsonCreator
    public TableScanNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("table") TableHandle table,
            @JsonProperty("outputVariables") List<VariableReferenceExpression> outputVariables,
            @JsonProperty("assignments") Map<VariableReferenceExpression, ColumnHandle> assignments)
    {
        super(sourceLocation, id);
        this.table = requireNonNull(table, "table is null");
        this.outputVariables = unmodifiableList(requireNonNull(outputVariables, "outputVariables is null"));
        this.assignments = unmodifiableMap(new HashMap<>(requireNonNull(assignments, "assignments is null")));
        checkArgument(assignments.keySet().containsAll(outputVariables), "assignments does not cover all of outputs");
        this.currentConstraint = null;
        this.enforcedConstraint = null;
        this.tableConstraints = emptyList();
    }

    public TableScanNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            TableHandle table,
            List<VariableReferenceExpression> outputVariables,
            Map<VariableReferenceExpression, ColumnHandle> assignments,
            TupleDomain<ColumnHandle> currentConstraint,
            TupleDomain<ColumnHandle> enforcedConstraint)
    {
        this (sourceLocation, id, table, outputVariables, assignments, emptyList(), currentConstraint, enforcedConstraint);
    }

    public TableScanNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            TableHandle table,
            List<VariableReferenceExpression> outputVariables,
            Map<VariableReferenceExpression, ColumnHandle> assignments,
            List<TableConstraint<ColumnHandle>> tableConstraints,
            TupleDomain<ColumnHandle> currentConstraint,
            TupleDomain<ColumnHandle> enforcedConstraint)
    {
        super(sourceLocation, id);
        this.table = requireNonNull(table, "table is null");
        this.outputVariables = unmodifiableList(requireNonNull(outputVariables, "outputVariables is null"));
        this.assignments = unmodifiableMap(new HashMap<>(requireNonNull(assignments, "assignments is null")));
        checkArgument(assignments.keySet().containsAll(outputVariables), "assignments does not cover all of outputs");
        this.currentConstraint = requireNonNull(currentConstraint, "currentConstraint is null");
        this.enforcedConstraint = requireNonNull(enforcedConstraint, "enforcedConstraint is null");
        if (!currentConstraint.isAll() || !enforcedConstraint.isAll()) {
            checkArgument(table.getLayout().isPresent(), "tableLayout must be present when currentConstraint or enforcedConstraint is non-trivial");
        }
        this.tableConstraints = requireNonNull(tableConstraints, "tableConstraints is null");
    }

    /**
     * Get the table handle provided by connector
     */
    @JsonProperty("table")
    public TableHandle getTable()
    {
        return table;
    }

    /**
     * Get table constraints defined by connector
     */
    public List<TableConstraint<ColumnHandle>> getTableConstraints()
    {
        return tableConstraints;
    }

    /**
     * Get the mapping from symbols to columns
     */
    @JsonProperty
    public Map<VariableReferenceExpression, ColumnHandle> getAssignments()
    {
        return assignments;
    }

    /**
     * A TupleDomain that represents a predicate that every row this TableScan node
     * produces is guaranteed to satisfy.
     * <p>
     * This guarantee can have different origins.
     * For example, it may be successful predicate push down, or inherent guarantee provided by the underlying data.
     *
     * currentConstraint will only be used in planner. It is not transported to worker thus will be null on worker.
     */
    @Nullable
    public TupleDomain<ColumnHandle> getCurrentConstraint()
    {
        return currentConstraint;
    }

    /**
     * A TupleDomain that represents a predicate that has been successfully pushed into
     * this TableScan node. In other words, predicates that were removed from filters
     * above the TableScan node because the TableScan node can guarantee it.
     * <p>
     * This field is used to make sure that predicates which were previously pushed down
     * do not get lost in subsequent refinements of the table layout.
     */
    public TupleDomain<ColumnHandle> getEnforcedConstraint()
    {
        // enforcedConstraint can be pretty complex. As a result, it may incur a significant cost to serialize, store, and transport.
        checkState(enforcedConstraint != null, "enforcedConstraint should only be used in planner. It is not transported to workers.");
        return enforcedConstraint;
    }

    @Override
    public List<PlanNode> getSources()
    {
        // table scan should be the leaf node
        return emptyList();
    }

    @Override
    public LogicalProperties computeLogicalProperties(LogicalPropertiesProvider logicalPropertiesProvider)
    {
        requireNonNull(logicalPropertiesProvider, "logicalPropertiesProvider cannot be null.");
        return logicalPropertiesProvider.getTableScanProperties(this);
    }

    @Override
    @JsonProperty
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableScan(this, context);
    }

    @Override
    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder(this.getClass().getSimpleName());
        stringBuilder.append(" {");
        stringBuilder.append("table='").append(table).append('\'');
        stringBuilder.append(", outputVariables='").append(outputVariables).append('\'');
        stringBuilder.append(", assignments='").append(assignments).append('\'');
        stringBuilder.append(", currentConstraint='").append(currentConstraint).append('\'');
        stringBuilder.append(", enforcedConstraint='").append(enforcedConstraint).append('\'');
        stringBuilder.append('}');
        return stringBuilder.toString();
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.isEmpty(), "newChildren is not empty");
        return this;
    }

    private static void checkArgument(boolean test, String errorMessage)
    {
        if (!test) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    private static void checkState(boolean test, String errorMessage)
    {
        if (!test) {
            throw new IllegalStateException(errorMessage);
        }
    }
}
