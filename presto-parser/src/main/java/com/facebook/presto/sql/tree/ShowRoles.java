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

public class ShowRoles
        extends Statement
{
    private final Optional<Identifier> catalog;
    private final boolean current;

    public ShowRoles(Optional<Identifier> catalog, boolean current)
    {
        this(Optional.empty(), catalog, current);
    }

    public ShowRoles(NodeLocation location, Optional<Identifier> catalog, boolean current)
    {
        this(Optional.of(location), catalog, current);
    }

    public ShowRoles(Optional<NodeLocation> location, Optional<Identifier> catalog, boolean current)
    {
        super(location);
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.current = current;
    }

    public Optional<Identifier> getCatalog()
    {
        return catalog;
    }

    public boolean isCurrent()
    {
        return current;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowRoles(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalog, current);
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
        ShowRoles o = (ShowRoles) obj;
        return Objects.equals(catalog, o.catalog) &&
                current == o.current;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalog", catalog)
                .add("current", current)
                .toString();
    }
}
