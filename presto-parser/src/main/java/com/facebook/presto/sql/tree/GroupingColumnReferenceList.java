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
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class GroupingColumnReferenceList
    extends Node
{
    private final List<QualifiedName> groupingColumns;

    public GroupingColumnReferenceList(NodeLocation location, List<QualifiedName> groupingColumns)
    {
        super(Optional.of(location));
        this.groupingColumns = requireNonNull(groupingColumns);
    }

    public List<QualifiedName> getGroupingColumns()
    {
        return groupingColumns;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("groupingColumns", groupingColumns)
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

        GroupingColumnReferenceList that = (GroupingColumnReferenceList) o;

        return groupingColumns.equals(that.groupingColumns);
    }

    @Override
    public int hashCode()
    {
        return groupingColumns.hashCode();
    }
}
