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

import com.google.common.base.Objects;
import com.google.common.base.Optional;

import javax.annotation.Nullable;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class QuerySpecification
        extends QueryBody
{
    private final Select select;
    private final List<Relation> from;
    private final Optional<Expression> where;
    private final List<Expression> groupBy;
    private final Optional<Expression> having;
    private final List<SortItem> orderBy;
    private final Optional<String> limit;

    public QuerySpecification(
            Select select,
            @Nullable List<Relation> from,
            Optional<Expression> where,
            List<Expression> groupBy,
            Optional<Expression> having,
            List<SortItem> orderBy,
            Optional<String> limit)
    {
        checkNotNull(select, "select is null");
        checkNotNull(where, "where is null");
        checkNotNull(groupBy, "groupBy is null");
        checkNotNull(having, "having is null");
        checkNotNull(orderBy, "orderBy is null");
        checkNotNull(limit, "limit is null");

        this.select = select;
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.having = having;
        this.orderBy = orderBy;
        this.limit = limit;
    }

    public Select getSelect()
    {
        return select;
    }

    public List<Relation> getFrom()
    {
        return from;
    }

    public Optional<Expression> getWhere()
    {
        return where;
    }

    public List<Expression> getGroupBy()
    {
        return groupBy;
    }

    public Optional<Expression> getHaving()
    {
        return having;
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
        return visitor.visitQuerySpecification(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("select", select)
                .add("from", from)
                .add("where", where.orNull())
                .add("groupBy", groupBy)
                .add("having", having.orNull())
                .add("orderBy", orderBy)
                .add("limit", limit.orNull())
                .toString();
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
        QuerySpecification o = (QuerySpecification) obj;
        return Objects.equal(select, o.select) &&
                Objects.equal(from, o.from) &&
                Objects.equal(where, o.where) &&
                Objects.equal(groupBy, o.groupBy) &&
                Objects.equal(having, o.having) &&
                Objects.equal(orderBy, o.orderBy) &&
                Objects.equal(limit, o.limit);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(select, from, where, groupBy, having, orderBy, limit);
    }
}
