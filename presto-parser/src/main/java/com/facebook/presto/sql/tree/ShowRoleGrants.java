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

public class ShowRoleGrants
        extends Statement
{
    private final Optional<Identifier> catalog;

    public ShowRoleGrants(Optional<Identifier> catalog)
    {
        this(Optional.empty(), catalog);
    }

    public ShowRoleGrants(NodeLocation location, Optional<Identifier> catalog)
    {
        this(Optional.of(location), catalog);
    }

    public ShowRoleGrants(Optional<NodeLocation> location, Optional<Identifier> catalog)
    {
        super(location);
        this.catalog = requireNonNull(catalog, "catalog is null");
    }

    public Optional<Identifier> getCatalog()
    {
        return catalog;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowRoleGrants(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalog);
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
        ShowRoleGrants o = (ShowRoleGrants) obj;
        return Objects.equals(catalog, o.catalog);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalog", catalog)
                .toString();
    }
}
