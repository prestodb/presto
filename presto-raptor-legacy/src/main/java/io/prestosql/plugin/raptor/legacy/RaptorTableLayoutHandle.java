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
package io.prestosql.plugin.raptor.legacy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class RaptorTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final RaptorTableHandle table;
    private final TupleDomain<ColumnHandle> constraint;
    private final Optional<RaptorPartitioningHandle> partitioning;

    @JsonCreator
    public RaptorTableLayoutHandle(
            @JsonProperty("table") RaptorTableHandle table,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("partitioning") Optional<RaptorPartitioningHandle> partitioning)
    {
        this.table = requireNonNull(table, "table is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.partitioning = requireNonNull(partitioning, "partitioning is null");
    }

    @JsonProperty
    public RaptorTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public Optional<RaptorPartitioningHandle> getPartitioning()
    {
        return partitioning;
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
