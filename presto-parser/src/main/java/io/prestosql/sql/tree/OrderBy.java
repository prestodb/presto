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
package io.prestosql.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class OrderBy
        extends Node
{
    private final List<SortItem> sortItems;

    public OrderBy(List<SortItem> sortItems)
    {
        this(Optional.empty(), sortItems);
    }

    public OrderBy(NodeLocation location, List<SortItem> sortItems)
    {
        this(Optional.of(location), sortItems);
    }

    private OrderBy(Optional<NodeLocation> location, List<SortItem> sortItems)
    {
        super(location);
        requireNonNull(sortItems, "sortItems is null");
        checkArgument(!sortItems.isEmpty(), "sortItems should not be empty");
        this.sortItems = ImmutableList.copyOf(sortItems);
    }

    public List<SortItem> getSortItems()
    {
        return sortItems;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitOrderBy(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return sortItems;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sortItems", sortItems)
                .toString();
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
        OrderBy o = (OrderBy) obj;
        return Objects.equals(sortItems, o.sortItems);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sortItems);
    }
}
