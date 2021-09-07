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

import static com.facebook.presto.sql.tree.CreateView.Security;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateMaterializedView
        extends Statement
{
    private final QualifiedName name;
    private final Query query;
    private final boolean notExists;
    private final List<Property> properties;
    private final Optional<String> comment;
    private final Optional<Security> security;

    public CreateMaterializedView(
            Optional<NodeLocation> location,
            QualifiedName name,
            Query query,
            boolean notExists,
            List<Property> properties,
            Optional<String> comment,
            Optional<Security> security)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.query = requireNonNull(query, "query is null");
        this.notExists = notExists;
        this.properties = ImmutableList.copyOf(requireNonNull(properties, "properties is null"));
        this.comment = requireNonNull(comment, "comment is null");
        this.security = requireNonNull(security, "security is null");
    }

    public QualifiedName getName()
    {
        return name;
    }

    public Query getQuery()
    {
        return query;
    }

    public boolean isNotExists()
    {
        return notExists;
    }

    public List<Property> getProperties()
    {
        return properties;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    public Optional<Security> getSecurity()
    {
        return security;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateMaterializedView(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.<Node>builder().add(query).addAll(properties).build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, query, notExists, properties, comment, security);
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
        CreateMaterializedView o = (CreateMaterializedView) obj;
        return Objects.equals(name, o.name)
                && Objects.equals(query, o.query)
                && Objects.equals(notExists, o.notExists)
                && Objects.equals(properties, o.properties)
                && Objects.equals(comment, o.comment)
                && Objects.equals(security, o.security);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("query", query)
                .add("notExists", notExists)
                .add("properties", properties)
                .add("comment", comment)
                .add("security", security)
                .toString();
    }
}
