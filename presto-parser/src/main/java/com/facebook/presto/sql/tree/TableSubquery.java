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

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class TableSubquery
        extends QueryBody
{
    private final Query query;

    public TableSubquery(Query query)
    {
        this(Optional.empty(), query);
    }

    public TableSubquery(NodeLocation location, Query query)
    {
        this(Optional.of(location), query);
    }

    private TableSubquery(Optional<NodeLocation> location, Query query)
    {
        super(location);
        this.query = query;
    }

    public Query getQuery()
    {
        return query;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableSubquery(this, context);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(query)
                .toString();
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

        TableSubquery tableSubquery = (TableSubquery) o;
        return Objects.equals(query, tableSubquery.query);
    }

    @Override
    public int hashCode()
    {
        return query.hashCode();
    }
}
