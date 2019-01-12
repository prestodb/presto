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
package io.prestosql.plugin.accumulo.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class AccumuloTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final AccumuloTableHandle table;
    private final TupleDomain<ColumnHandle> constraint;

    @JsonCreator
    public AccumuloTableLayoutHandle(@JsonProperty("table") AccumuloTableHandle table,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint)
    {
        this.table = requireNonNull(table, "table is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    @JsonProperty
    public AccumuloTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
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

        AccumuloTableLayoutHandle other = (AccumuloTableLayoutHandle) obj;
        return Objects.equals(table, other.table)
                && Objects.equals(constraint, other.constraint);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, constraint);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("constraint", constraint)
                .toString();
    }
}
