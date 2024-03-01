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

public class RefreshMaterializedView
        extends Statement
{
    private final Table target;
    private final Expression where;

    public RefreshMaterializedView(Table target, Expression where)
    {
        this(Optional.empty(), target, where);
    }

    public RefreshMaterializedView(NodeLocation location, Table target, Expression where)
    {
        this(Optional.of(location), target, where);
    }

    private RefreshMaterializedView(Optional<NodeLocation> location, Table target, Expression where)
    {
        super(location);
        this.target = requireNonNull(target, "target is null");
        this.where = requireNonNull(where, "where is null");
    }

    public Table getTarget()
    {
        return target;
    }

    public Expression getWhere()
    {
        return where;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRefreshMaterializedView(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(where);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(where);
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
        RefreshMaterializedView o = (RefreshMaterializedView) obj;
        return Objects.equals(target, o.target) &&
                Objects.equals(where, o.where);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", target)
                .add("where", where)
                .toString();
    }
}
