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
package io.prestosql.plugin.kudu;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class KuduTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final KuduTableHandle tableHandle;
    private final TupleDomain<ColumnHandle> constraintSummary;
    private final Optional<Set<ColumnHandle>> desiredColumns;

    @JsonCreator
    public KuduTableLayoutHandle(@JsonProperty("tableHandle") KuduTableHandle tableHandle,
            @JsonProperty("constraintSummary") TupleDomain<ColumnHandle> constraintSummary,
            @JsonProperty("desiredColumns") Optional<Set<ColumnHandle>> desiredColumns)
    {
        this.tableHandle = requireNonNull(tableHandle, "table is null");
        this.constraintSummary = constraintSummary;
        this.desiredColumns = desiredColumns;
    }

    @JsonProperty
    public KuduTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraintSummary()
    {
        return constraintSummary;
    }

    @JsonProperty
    public Optional<Set<ColumnHandle>> getDesiredColumns()
    {
        return desiredColumns;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        KuduTableLayoutHandle other = (KuduTableLayoutHandle) obj;
        return Objects.equals(tableHandle, other.tableHandle)
                && Objects.equals(constraintSummary, other.constraintSummary)
                && Objects.equals(desiredColumns, other.desiredColumns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableHandle,
                constraintSummary,
                desiredColumns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableHandle", tableHandle)
                .add("constraintSummary", constraintSummary)
                .add("desiredColumns", desiredColumns)
                .toString();
    }
}
