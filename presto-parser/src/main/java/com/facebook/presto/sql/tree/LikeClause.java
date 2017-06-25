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

public final class LikeClause
        extends TableElement
{
    private final QualifiedName tableName;
    private final Optional<PropertiesOption> propertiesOption;

    public enum PropertiesOption
    {
        INCLUDING,
        EXCLUDING
    }

    public LikeClause(QualifiedName tableName, Optional<PropertiesOption> propertiesOption)
    {
        this(Optional.empty(), tableName, propertiesOption);
    }

    public LikeClause(NodeLocation location, QualifiedName tableName, Optional<PropertiesOption> propertiesOption)
    {
        this(Optional.of(location), tableName, propertiesOption);
    }

    private LikeClause(Optional<NodeLocation> location, QualifiedName tableName, Optional<PropertiesOption> propertiesOption)
    {
        super(location);
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.propertiesOption = requireNonNull(propertiesOption, "propertiesOption is null");
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public Optional<PropertiesOption> getPropertiesOption()
    {
        return propertiesOption;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLikeClause(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        LikeClause o = (LikeClause) obj;
        return Objects.equals(this.tableName, o.tableName) &&
                Objects.equals(this.propertiesOption, o.propertiesOption);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, propertiesOption);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableName", tableName)
                .add("propertiesOption", propertiesOption)
                .toString();
    }
}
