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

public class ShowStats
        extends Statement
{
    private final QualifiedName table;
    private final Optional<Expression> where;
    private final List<SortItem> orderBy;
    private final Optional<String> limit;

    public ShowStats(QualifiedName table)
    {
        this(Optional.empty(), table, Optional.empty(), ImmutableList.of(), Optional.empty());
    }

    public ShowStats(QualifiedName table, Optional<Expression> where, List<SortItem> orderBy, Optional<String> limit)
    {
        this(Optional.empty(), table, where, orderBy, limit);
    }

    public ShowStats(NodeLocation location, QualifiedName table)
    {
        this(Optional.of(location), table, Optional.empty(), ImmutableList.of(), Optional.empty());
    }

    public ShowStats(NodeLocation location, QualifiedName table, Optional<Expression> where, List<SortItem> orderBy, Optional<String> limit)
    {
        this(Optional.empty(), table, where, orderBy, limit);
    }

    private ShowStats(Optional<NodeLocation> location, QualifiedName table, Optional<Expression> where, List<SortItem> orderBy, Optional<String> limit)
    {
        super(location);
        this.table = table;
        this.where = where;
        this.orderBy = orderBy;
        this.limit = limit;
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
        return visitor.visitShowStats(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table);
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
        ShowStats o = (ShowStats) obj;
        return Objects.equals(table, o.table) &&
                Objects.equals(where, where) &&
                Objects.equals(orderBy, orderBy) &&
                Objects.equals(limit, limit);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .toString();
    }
}
