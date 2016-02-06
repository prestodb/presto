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

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class SortItem
        extends Node
{
    public enum Ordering
    {
        ASCENDING, DESCENDING
    }

    public enum NullOrdering
    {
        FIRST, LAST, UNDEFINED
    }

    private final Expression sortKey;
    private final Ordering ordering;
    private final NullOrdering nullOrdering;

    public SortItem(Expression sortKey, Ordering ordering, NullOrdering nullOrdering)
    {
        this(Optional.empty(), sortKey, ordering, nullOrdering);
    }

    public SortItem(NodeLocation location, Expression sortKey, Ordering ordering, NullOrdering nullOrdering)
    {
        this(Optional.of(location), sortKey, ordering, nullOrdering);
    }

    private SortItem(Optional<NodeLocation> location, Expression sortKey, Ordering ordering, NullOrdering nullOrdering)
    {
        super(location);
        this.ordering = ordering;
        this.sortKey = sortKey;
        this.nullOrdering = nullOrdering;
    }

    public Expression getSortKey()
    {
        return sortKey;
    }

    public Ordering getOrdering()
    {
        return ordering;
    }

    public NullOrdering getNullOrdering()
    {
        return nullOrdering;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSortItem(this, context);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sortKey", sortKey)
                .add("ordering", ordering)
                .add("nullOrdering", nullOrdering)
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

        SortItem sortItem = (SortItem) o;
        return Objects.equals(sortKey, sortItem.sortKey) &&
                (ordering == sortItem.ordering) &&
                (nullOrdering == sortItem.nullOrdering);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sortKey, ordering, nullOrdering);
    }
}
