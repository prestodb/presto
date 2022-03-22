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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.InternalPlanNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class CanonicalTableScanNode
        extends InternalPlanNode
{
    private final CanonicalTableHandle table;
    private final Map<VariableReferenceExpression, ColumnHandle> assignments;
    private final List<VariableReferenceExpression> outputVariables;

    @JsonCreator
    public CanonicalTableScanNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("table") CanonicalTableHandle table,
            @JsonProperty("outputVariables") List<VariableReferenceExpression> outputVariables,
            @JsonProperty("assignments") Map<VariableReferenceExpression, ColumnHandle> assignments)
    {
        super(id);
        this.table = requireNonNull(table, "table is null");
        this.outputVariables = unmodifiableList(requireNonNull(outputVariables, "outputVariables is null"));
        this.assignments = unmodifiableMap(new HashMap<>(requireNonNull(assignments, "assignments is null")));
        checkArgument(assignments.keySet().containsAll(outputVariables), "assignments does not cover all of outputs");
    }

    @Override
    public List<PlanNode> getSources()
    {
        return emptyList();
    }

    @Override
    @JsonProperty
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.isEmpty(), "newChildren is not empty");
        return this;
    }

    @JsonProperty
    public CanonicalTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public Map<VariableReferenceExpression, ColumnHandle> getAssignments()
    {
        return assignments;
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitCanonicalTableScan(this, context);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CanonicalTableScanNode that = (CanonicalTableScanNode) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(assignments, that.assignments) &&
                Objects.equals(outputVariables, that.outputVariables);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, assignments, outputVariables);
    }

    public static class CanonicalTableHandle
    {
        private final ConnectorId connectorId;
        private final ConnectorTableHandle tableHandle;
        private final Optional<Object> layoutIdentifier;

        // This field is only used for plan rewrite but not participate in fragment result caching, so
        // we do not serialize this field.
        private final Optional<ConnectorTableLayoutHandle> layoutHandle;

        public static CanonicalTableHandle getCanonicalTableHandle(TableHandle tableHandle)
        {
            return new CanonicalTableHandle(
                    tableHandle.getConnectorId(),
                    tableHandle.getConnectorHandle(),
                    tableHandle.getLayout().map(layout -> layout.getIdentifier(Optional.empty())),
                    tableHandle.getLayout());
        }

        @JsonCreator
        public CanonicalTableHandle(
                @JsonProperty("connectorId") ConnectorId connectorId,
                @JsonProperty("tableHandle") ConnectorTableHandle tableHandle,
                @JsonProperty("layoutIdentifier") Optional<Object> layoutIdentifier)
        {
            this(connectorId, tableHandle, layoutIdentifier, Optional.empty());
        }

        public CanonicalTableHandle(
                ConnectorId connectorId,
                ConnectorTableHandle tableHandle,
                Optional<Object> layoutIdentifier,
                Optional<ConnectorTableLayoutHandle> layoutHandle)
        {
            this.connectorId = requireNonNull(connectorId, "connectorId is null");
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
            this.layoutIdentifier = requireNonNull(layoutIdentifier, "layoutIdentifier is null");
            this.layoutHandle = requireNonNull(layoutHandle, "layoutHandle is null");
        }

        @JsonProperty
        public ConnectorId getConnectorId()
        {
            return connectorId;
        }

        @JsonProperty
        public ConnectorTableHandle getTableHandle()
        {
            return tableHandle;
        }

        @JsonProperty
        public Optional<Object> getLayoutIdentifier()
        {
            return layoutIdentifier;
        }

        public Optional<ConnectorTableLayoutHandle> getLayoutHandle()
        {
            return layoutHandle;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CanonicalTableHandle that = (CanonicalTableHandle) o;
            return Objects.equals(connectorId, that.connectorId) &&
                    Objects.equals(tableHandle, that.tableHandle) &&
                    Objects.equals(layoutIdentifier, that.layoutIdentifier);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(connectorId, tableHandle, layoutIdentifier);
        }
    }
}
