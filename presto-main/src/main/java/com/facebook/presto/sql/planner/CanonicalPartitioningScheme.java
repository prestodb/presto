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

import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.sql.planner.RowExpressionVariableInliner.inlineVariables;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class CanonicalPartitioningScheme
{
    private final Optional<ConnectorId> connectorId;
    private final ConnectorPartitioningHandle connectorHandle;
    private final List<RowExpression> arguments;
    private final List<VariableReferenceExpression> outputLayout;

    public static CanonicalPartitioningScheme getCanonicalPartitioningScheme(
            PartitioningScheme partitioningScheme,
            Map<VariableReferenceExpression, VariableReferenceExpression> originalToNewVariableNames)
    {
        return new CanonicalPartitioningScheme(
                partitioningScheme.getPartitioning().getHandle().getConnectorId(),
                partitioningScheme.getPartitioning().getHandle().getConnectorHandle(),
                partitioningScheme.getPartitioning().getArguments().stream()
                        .map(argument -> inlineVariables(originalToNewVariableNames, argument))
                        .collect(toImmutableList()),
                partitioningScheme.getOutputLayout().stream()
                        .map(originalToNewVariableNames::get)
                        .collect(toImmutableList()));
    }

    @JsonCreator
    CanonicalPartitioningScheme(
            @JsonProperty("connectorId") Optional<ConnectorId> connectorId,
            @JsonProperty("connectorHandle") ConnectorPartitioningHandle connectorHandle,
            @JsonProperty("arguments") List<RowExpression> arguments,
            @JsonProperty("outputLayout") List<VariableReferenceExpression> outputLayout)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.connectorHandle = requireNonNull(connectorHandle, "connectorHandle is null");
        this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
        this.outputLayout = ImmutableList.copyOf(requireNonNull(outputLayout, "outputLayout is null"));
    }

    @JsonProperty
    public Optional<ConnectorId> getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public ConnectorPartitioningHandle getConnectorHandle()
    {
        return connectorHandle;
    }

    @JsonProperty
    public List<RowExpression> getArguments()
    {
        return arguments;
    }

    @JsonProperty
    public List<VariableReferenceExpression> getOutputLayout()
    {
        return outputLayout;
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
        CanonicalPartitioningScheme that = (CanonicalPartitioningScheme) o;
        return Objects.equals(connectorId, that.connectorId) &&
                Objects.equals(connectorHandle, that.connectorHandle) &&
                Objects.equals(arguments, that.arguments) &&
                Objects.equals(outputLayout, that.outputLayout);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, connectorHandle, arguments, outputLayout);
    }
}
