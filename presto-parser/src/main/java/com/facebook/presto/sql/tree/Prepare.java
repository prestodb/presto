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
import static java.util.Objects.requireNonNull;

public class Prepare
        extends Statement
{
    private final String name;
    private final Query query;

    public Prepare(NodeLocation location, String name, Query query)
    {
        this(Optional.of(location), name, query);
    }

    public Prepare(String name, Query query)
    {
        this(Optional.empty(), name, query);
    }

    private Prepare(Optional<NodeLocation> location, String name, Query query)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.query = requireNonNull(query, "query is null");
    }

    public String getName()
    {
        return name;
    }

    public Query getQuery()
    {
        return query;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitPrepare(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, query);
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
        Prepare o = (Prepare) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(query, o.query);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("query", query)
                .toString();
    }
}
