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

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.util.List;

public class AliasedRelation
        extends Relation
{
    private final Relation relation;
    private final String alias;
    private final List<String> columnNames;

    public AliasedRelation(Relation relation, String alias, List<String> columnNames)
    {
        Preconditions.checkNotNull(relation, "relation is null");
        Preconditions.checkNotNull(alias, " is null");

        this.relation = relation;
        this.alias = alias;
        this.columnNames = columnNames;
    }

    public Relation getRelation()
    {
        return relation;
    }

    public String getAlias()
    {
        return alias;
    }

    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAliasedRelation(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("relation", relation)
                .add("alias", alias)
                .add("columnNames", columnNames)
                .omitNullValues()
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

        AliasedRelation that = (AliasedRelation) o;

        if (!alias.equals(that.alias)) {
            return false;
        }
        if (columnNames != null ? !columnNames.equals(that.columnNames) : that.columnNames != null) {
            return false;
        }
        if (!relation.equals(that.relation)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = relation.hashCode();
        result = 31 * result + alias.hashCode();
        result = 31 * result + (columnNames != null ? columnNames.hashCode() : 0);
        return result;
    }

    public static Function<QualifiedName, QualifiedName> applyAlias(final AliasedRelation node)
    {
        return new Function<QualifiedName, QualifiedName>()
        {
            @Override
            public QualifiedName apply(QualifiedName input)
            {
                return QualifiedName.of(node.getAlias(), input.getSuffix()); // TODO: handle column aliases
            }
        };
    }
}
