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

import static com.google.common.base.Preconditions.checkNotNull;

public class CreateMaterializedView
        extends Statement
{
    private final QualifiedName name;
    private final Optional<String> refresh;
    private final Query tableDefinition;

    public CreateMaterializedView(QualifiedName name, Optional<String> refresh, Query tableDefinition)
    {
        this.name = checkNotNull(name, "name is null");
        this.refresh = checkNotNull(refresh, "refresh is null");
        this.tableDefinition = checkNotNull(tableDefinition, "tableDefinition is null");
    }

    public QualifiedName getName()
    {
        return name;
    }

    public Optional<String> getRefresh()
    {
        return refresh;
    }

    public Query getTableDefinition()
    {
        return tableDefinition;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateMaterializedView(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, refresh, tableDefinition);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        else if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        CreateMaterializedView o = (CreateMaterializedView) obj;
        return Objects.equal(name, o.name)
                && Objects.equal(refresh, o.refresh)
                && Objects.equal(tableDefinition, o.tableDefinition);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("refresh", refresh)
                .add("tableDefinition", tableDefinition)
                .toString();
    }
}
