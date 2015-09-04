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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Query
        extends Statement
{
    private final Optional<With> with;
    private final QueryBody queryBody;
    private final List<SortItem> orderBy;
    private final Optional<String> limit;
    private final Optional<Approximate> approximate;

    public Query(
            Optional<With> with,
            QueryBody queryBody,
            List<SortItem> orderBy,
            Optional<String> limit,
            Optional<Approximate> approximate)
    {
        requireNonNull(with, "with is null");
        requireNonNull(queryBody, "queryBody is null");
        requireNonNull(orderBy, "orderBy is null");
        requireNonNull(limit, "limit is null");
        requireNonNull(approximate, "approximate is null");

        this.with = with;
        this.queryBody = queryBody;
        this.orderBy = orderBy;
        this.limit = limit;
        this.approximate = approximate;
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

    public Optional<Approximate> getApproximate()
    {
        return approximate;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitQuery(this, context);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("with", with.orElse(null))
                .add("queryBody", queryBody)
                .add("orderBy", orderBy)
                .add("limit", limit.orElse(null))
                .add("approximate", approximate.orElse(null))
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
        return Objects.equals(with, o.with) &&
                Objects.equals(queryBody, o.queryBody) &&
                Objects.equals(orderBy, o.orderBy) &&
                Objects.equals(limit, o.limit) &&
                Objects.equals(approximate, o.approximate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(with, queryBody, orderBy, limit, approximate);
    }
}
