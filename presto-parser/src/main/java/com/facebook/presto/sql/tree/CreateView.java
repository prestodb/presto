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
    public enum Security {
        INVOKER, DEFINER
    }

    private final QualifiedName name;
    private final Query query;
    private final boolean replace;
    private final Optional<Security> security;

    public CreateView(QualifiedName name, Query query, boolean replace, Optional<Security> security)
    {
        this(Optional.empty(), name, query, replace, security);
    }

    public CreateView(NodeLocation location, QualifiedName name, Query query, boolean replace, Optional<Security> security)
    {
        this(Optional.of(location), name, query, replace, security);
    }

    private CreateView(Optional<NodeLocation> location, QualifiedName name, Query query, boolean replace, Optional<Security> security)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.query = requireNonNull(query, "query is null");
        this.replace = replace;
        this.security = requireNonNull(security, "security is null");
    }

    public QualifiedName getName()
    {
        return name;
    }

    public Optional<Security> getSecurity()
    {
        return security;
    }

    public Query getQuery()
    {
        return query;
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
        return Objects.hash(name, query, replace, security);
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
                && Objects.equals(replace, o.replace)
                && Objects.equals(security, o.security);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("query", query)
                .add("replace", replace)
                .add("security", security)
                .toString();
    }
}
