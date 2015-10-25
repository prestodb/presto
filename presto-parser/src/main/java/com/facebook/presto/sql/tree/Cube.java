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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class Cube
        extends GroupBySpecification
{
    private final List<Expression> columnList;

    public Cube(NodeLocation location, GroupingColumnReferenceList columns)
    {
        super(Optional.of(location));
        requireNonNull(columns, "columns is null");
        columnList = columns.getGroupingColumns().stream()
                .map(QualifiedNameReference::new)
                .collect(toList());
    }

    public List<Expression> getColumnList()
    {
        return columnList;
    }

    @Override
    public List<List<Expression>> enumerateGroupingSets()
    {
        Set<Set<Expression>> set = Sets.powerSet(ImmutableSet.copyOf(columnList));
        return set.stream()
                .map(qualifiedNames -> qualifiedNames.stream()
                        .collect(toList()))
                .collect(toList());
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
        Cube cube = (Cube) o;
        return Objects.equals(columnList, cube.columnList);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnList);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnList", columnList)
                .toString();
    }
}
