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
        extends PlanNode
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
        private final ConnectorTableHandle connectorHandle;

        private final Optional<Object> layoutIdentifier;

        public static CanonicalTableHandle getCanonicalTableHandle(TableHandle tableHandle)
        {
            return new CanonicalTableHandle(tableHandle.getConnectorId(), tableHandle.getConnectorHandle(), tableHandle.getLayout().map(ConnectorTableLayoutHandle::getIdentifier));
        }

        @JsonCreator
        public CanonicalTableHandle(
                @JsonProperty("coonectorId") ConnectorId connectorId,
                @JsonProperty("connectorHandle") ConnectorTableHandle connectorHandle,
                @JsonProperty("layoutIdentifier") Optional<Object> layoutIdentifier)
        {
            this.connectorId = requireNonNull(connectorId, "connectorId is null");
            this.connectorHandle = requireNonNull(connectorHandle, "connectorHandle is null");
            this.layoutIdentifier = requireNonNull(layoutIdentifier, "layoutIdentifier is null");
        }

        @JsonProperty
        public ConnectorId getConnectorId()
        {
            return connectorId;
        }

        @JsonProperty
        public ConnectorTableHandle getConnectorHandle()
        {
            return connectorHandle;
        }

        @JsonProperty
        public Optional<Object> getLayoutIdentifier()
        {
            return layoutIdentifier;
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
                    Objects.equals(connectorHandle, that.connectorHandle) &&
                    Objects.equals(layoutIdentifier, that.layoutIdentifier);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(connectorId, connectorHandle, layoutIdentifier);
        }
    }
}
