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

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateTableAsSelect
        extends Statement
{
    private final QualifiedName name;
    private final Query query;
    private final Map<String, Expression> properties;
    private final boolean withData;

    public CreateTableAsSelect(QualifiedName name, Query query, Map<String, Expression> properties, boolean withData)
    {
        this.name = requireNonNull(name, "name is null");
        this.query = requireNonNull(query, "query is null");
        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
        this.withData = withData;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public Query getQuery()
    {
        return query;
    }

    public Map<String, Expression> getProperties()
    {
        return properties;
    }

    public boolean getWithData()
    {
        return withData;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateTableAsSelect(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, query, properties);
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
        CreateTableAsSelect o = (CreateTableAsSelect) obj;
        return Objects.equals(name, o.name)
                && Objects.equals(query, o.query)
                && Objects.equals(properties, o.properties);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("query", query)
                .add("properties", properties)
                .add("withData", withData)
                .toString();
    }
}
