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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.google.common.base.MoreObjects.toStringHelper;

public class ShowStats
        extends Statement
{
    private final Query query;

    @VisibleForTesting
    public ShowStats(QualifiedName table)
    {
        this(Optional.empty(), table);
    }

    @VisibleForTesting
    public ShowStats(Query query)
    {
        this(Optional.empty(), query);
    }

    public ShowStats(Optional<NodeLocation> location, QualifiedName table)
    {
        this(location, createFakeQuery(location, table));
    }

    public ShowStats(Optional<NodeLocation> location, Query query)
    {
        super(location);
        this.query = query;
    }

    private static Query createFakeQuery(Optional<NodeLocation> location, QualifiedName name)
    {
        Select select = location.map(l -> new Select(l, false, ImmutableList.of(new AllColumns())))
                .orElse(new Select(false, ImmutableList.of(new AllColumns())));
        Relation relation = new Table(name);
        return simpleQuery(select, relation);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowStats(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(query);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(query);
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
        return Objects.equals(query, o.query);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("query", query)
                .toString();
    }

    public Query getQuery()
    {
        return query;
    }
}
