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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.ExpressionFormatter.formatSortItems;
import static java.util.Objects.requireNonNull;

public class TableArgument
        extends Node
{
    private final Relation table;
    private final Optional<List<Expression>> partitionBy; // it is allowed to partition by empty list
    private final Optional<OrderBy> orderBy;
    private final boolean pruneWhenEmpty;

    public TableArgument(
            NodeLocation location,
            Relation table,
            Optional<List<Expression>> partitionBy,
            Optional<OrderBy> orderBy,
            boolean pruneWhenEmpty)
    {
        super(Optional.of(location));
        this.table = requireNonNull(table, "table is null");
        this.partitionBy = requireNonNull(partitionBy, "partitionBy is null");
        this.orderBy = requireNonNull(orderBy, "orderBy is null");
        this.pruneWhenEmpty = pruneWhenEmpty;
    }

    public Relation getTable()
    {
        return table;
    }

    public Optional<List<Expression>> getPartitionBy()
    {
        return partitionBy;
    }

    public Optional<OrderBy> getOrderBy()
    {
        return orderBy;
    }

    public boolean isPruneWhenEmpty()
    {
        return pruneWhenEmpty;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableArgument(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> builder = ImmutableList.builder();
        builder.add(table);
        partitionBy.ifPresent(builder::addAll);
        orderBy.ifPresent(builder::add);

        return builder.build();
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

        TableArgument other = (TableArgument) o;
        return Objects.equals(table, other.table) &&
                Objects.equals(partitionBy, other.partitionBy) &&
                Objects.equals(orderBy, other.orderBy) &&
                pruneWhenEmpty == other.pruneWhenEmpty;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, partitionBy, orderBy, pruneWhenEmpty);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(table);
        partitionBy.ifPresent(partitioning -> builder.append(partitioning.stream()
                .map(Expression::toString)
                .collect(Collectors.joining(", ", " PARTITION BY (", ")"))));
        orderBy.ifPresent(ordering -> builder.append(" ORDER BY (")
                .append(formatSortItems(ordering.getSortItems(), Optional.empty()))
                .append(")"));

        return builder.toString();
    }
}
