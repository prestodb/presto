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
package io.prestosql.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class WithQuery
        extends Node
{
    private final Identifier name;
    private final Query query;
    private final Optional<List<Identifier>> columnNames;

    public WithQuery(Identifier name, Query query, Optional<List<Identifier>> columnNames)
    {
        this(Optional.empty(), name, query, columnNames);
    }

    public WithQuery(NodeLocation location, Identifier name, Query query, Optional<List<Identifier>> columnNames)
    {
        this(Optional.of(location), name, query, columnNames);
    }

    private WithQuery(Optional<NodeLocation> location, Identifier name, Query query, Optional<List<Identifier>> columnNames)
    {
        super(location);
        this.name = name;
        this.query = requireNonNull(query, "query is null");
        this.columnNames = requireNonNull(columnNames, "columnNames is null");
    }

    public Identifier getName()
    {
        return name;
    }

    public Query getQuery()
    {
        return query;
    }

    public Optional<List<Identifier>> getColumnNames()
    {
        return columnNames;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitWithQuery(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(query);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("query", query)
                .add("columnNames", columnNames)
                .omitNullValues()
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, query, columnNames);
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
        WithQuery o = (WithQuery) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(query, o.query) &&
                Objects.equals(columnNames, o.columnNames);
    }
}
