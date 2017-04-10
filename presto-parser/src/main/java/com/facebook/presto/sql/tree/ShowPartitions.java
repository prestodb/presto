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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ShowPartitions
        extends Statement
{
    private final QualifiedName table;
    private final Optional<Expression> where;
    private final List<SortItem> orderBy;
    private final Optional<String> limit;

    public ShowPartitions(QualifiedName table, Optional<Expression> where, List<SortItem> orderBy, Optional<String> limit)
    {
        this(Optional.empty(), table, where, orderBy, limit);
    }

    public ShowPartitions(NodeLocation location, QualifiedName table, Optional<Expression> where, List<SortItem> orderBy, Optional<String> limit)
    {
        this(Optional.of(location), table, where, orderBy, limit);
    }

    private ShowPartitions(Optional<NodeLocation> location, QualifiedName table, Optional<Expression> where, List<SortItem> orderBy, Optional<String> limit)
    {
        super(location);
        this.table = requireNonNull(table, "table is null");
        this.where = requireNonNull(where, "where is null");
        this.orderBy = ImmutableList.copyOf(requireNonNull(orderBy, "orderBy is null"));
        this.limit = requireNonNull(limit, "limit is null");
    }

    public QualifiedName getTable()
    {
        return table;
    }

    public Optional<Expression> getWhere()
    {
        return where;
    }

    public List<SortItem> getOrderBy()
    {
        return orderBy;
    }

    public Optional<String> getLimit()
    {
        return limit;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowPartitions(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        where.ifPresent(nodes::add);
        nodes.addAll(orderBy);
        return nodes.build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, where, orderBy, limit);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ShowPartitions o = (ShowPartitions) obj;
        return Objects.equals(table, o.table) &&
                Objects.equals(where, o.where) &&
                Objects.equals(orderBy, o.orderBy) &&
                Objects.equals(limit, o.limit);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("where", where)
                .add("orderBy", orderBy)
                .add("limit", limit)
                .toString();
    }
}
