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
package com.facebook.presto.connector.thrift;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.relation.RowExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ThriftTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final String schemaName;
    private final String tableName;
    private final Optional<Set<ColumnHandle>> columns;
    private final TupleDomain<ColumnHandle> constraint;
    private final RowExpression remainingPredicate;

    @JsonCreator
    public ThriftTableLayoutHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columns") Optional<Set<ColumnHandle>> columns,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("remainingPredicate") RowExpression remainingPredicate)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columns = requireNonNull(columns, "columns is null").map(ImmutableSet::copyOf);
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.remainingPredicate = requireNonNull(remainingPredicate, "remainingPredicate is null");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public Optional<Set<ColumnHandle>> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public RowExpression getRemainingPredicate()
    {
        return remainingPredicate;
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
        ThriftTableLayoutHandle other = (ThriftTableLayoutHandle) o;
        return schemaName.equals(other.schemaName)
                && tableName.equals(other.tableName)
                && columns.equals(other.columns)
                && constraint.equals(other.constraint)
                && remainingPredicate.equals(other.remainingPredicate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, columns, constraint, remainingPredicate);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("remainingPredicate", TRUE_CONSTANT.equals(remainingPredicate) ? null : remainingPredicate.toString())
                .toString();
    }
}
