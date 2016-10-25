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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Collections.emptyList;

public class ShowStats
        extends Statement
{
    private final QualifiedName table;

    public ShowStats(QualifiedName table)
    {
        this(Optional.empty(), table);
    }

    public ShowStats(NodeLocation location, QualifiedName table)
    {
        this(Optional.of(location), table);
    }

    private ShowStats(Optional<NodeLocation> location, QualifiedName table)
    {
        super(location);
        this.table = table;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowStats(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return emptyList();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table);
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
        ShowStats o = (ShowStats) obj;
        return Objects.equals(table, o.table);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .toString();
    }

    public QualifiedName getTable()
    {
        return table;
    }
}
