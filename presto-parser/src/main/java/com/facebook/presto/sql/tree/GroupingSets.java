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
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;

public class GroupingSets
        extends GroupBySpecification
{
    private final List<List<Expression>> sets;

    public GroupingSets(NodeLocation location, List<GroupingColumnReferenceList> groupingSetList)
    {
        super(Optional.of(location));
        sets = groupingSetList.stream()
                .map(groupingColumnReferenceList -> groupingColumnReferenceList.getGroupingColumns().stream()
                        .map(QualifiedNameReference::new)
                        .collect(Collectors.<Expression>toList()))
                .collect(Collectors.toList());
    }

    @Override
    public List<List<Expression>> enumerateGroupingSets()
    {
        return sets;
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
        GroupingSets groupingSets = (GroupingSets) o;
        return Objects.equals(sets, groupingSets.sets);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sets);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sets", sets)
                .toString();
    }
}
