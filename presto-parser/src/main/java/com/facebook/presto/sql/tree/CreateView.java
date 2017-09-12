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

public class CreateView
        extends Statement
{
    private final QualifiedName name;
    private final Query query;
    private final Optional<String> comment;
    private final boolean replace;

    public CreateView(QualifiedName name, Query query, Optional<String> comment, boolean replace)
    {
        this(Optional.empty(), name, query, comment, replace);
    }

    public CreateView(NodeLocation location, QualifiedName name, Query query, Optional<String> comment, boolean replace)
    {
        this(Optional.of(location), name, query, comment, replace);
    }

    private CreateView(Optional<NodeLocation> location, QualifiedName name, Query query, Optional<String> comment, boolean replace)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.query = requireNonNull(query, "query is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.replace = replace;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public Query getQuery()
    {
        return query;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    public boolean isReplace()
    {
        return replace;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateView(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(query);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, query, comment, replace);
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
        CreateView o = (CreateView) obj;
        return Objects.equals(name, o.name)
                && Objects.equals(query, o.query)
                && Objects.equals(comment, o.comment)
                && Objects.equals(replace, o.replace);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("query", query)
                .add("comment", comment)
                .add("replace", replace)
                .toString();
    }
}
