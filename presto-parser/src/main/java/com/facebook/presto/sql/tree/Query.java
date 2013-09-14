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

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class Query
        extends Statement
{
    private final Optional<With> with;
    private final QueryBody queryBody;
    private final List<SortItem> orderBy;
    private final Optional<String> limit;

    public Query(
            Optional<With> with,
            QueryBody queryBody,
            List<SortItem> orderBy,
            Optional<String> limit)
    {
        checkNotNull(with, "with is null");
        checkNotNull(queryBody, "queryBody is null");
        checkNotNull(orderBy, "orderBy is null");
        checkNotNull(limit, "limit is null");

        this.with = with;
        this.queryBody = queryBody;
        this.orderBy = orderBy;
        this.limit = limit;
    }

    public Optional<With> getWith()
    {
        return with;
    }

    public QueryBody getQueryBody()
    {
        return queryBody;
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
        return visitor.visitQuery(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("with", with.orNull())
                .add("queryBody", queryBody)
                .add("orderBy", orderBy)
                .add("limit", limit.orNull())
                .omitNullValues()
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
        Query o = (Query) obj;
        return Objects.equal(with, o.with) &&
                Objects.equal(queryBody, o.queryBody) &&
                Objects.equal(orderBy, o.orderBy) &&
                Objects.equal(limit, o.limit);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(with, queryBody, orderBy, limit);
    }
}
