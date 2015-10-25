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

public class GroupByClause
        extends Node
{
    private final Optional<List<Expression>> ordinaryGroupingSet;
    private final Optional<List<Expression>> rollupList;
    private final Optional<List<Expression>> cubeList;
    private final Optional<List<GroupingColumnReferenceList>> groupingSetsSpecification;
    private final boolean isEmptyGroupingSet;

    public GroupByClause(
            Optional<List<Expression>> ordinaryGroupingSet,
            Optional<List<Expression>> rollupList,
            Optional<List<Expression>> cubeList,
            Optional<List<GroupingColumnReferenceList>> groupingSetsSpecification,
            boolean isEmptyGroupingSet)
    {
        this.ordinaryGroupingSet = ordinaryGroupingSet;
        this.rollupList = rollupList;
        this.cubeList = cubeList;
        this.groupingSetsSpecification = groupingSetsSpecification;
        this.isEmptyGroupingSet = isEmptyGroupingSet;
    }

    public Optional<List<Expression>> getOrdinaryGroupingSet()
    {
        return ordinaryGroupingSet;
    }

    public Optional<List<Expression>> getRollupList()
    {
        return rollupList;
    }

    public Optional<List<Expression>> getCubeList()
    {
        return cubeList;
    }

    public Optional<List<GroupingColumnReferenceList>> getGroupingSetsSpecification()
    {
        return groupingSetsSpecification;
    }

    public boolean isEmptyGroupingSet()
    {
        return isEmptyGroupingSet;
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

        GroupByClause that = (GroupByClause) o;

        if (isEmptyGroupingSet != that.isEmptyGroupingSet) {
            return false;
        }
        if (!ordinaryGroupingSet.equals(that.ordinaryGroupingSet)) {
            return false;
        }
        if (!rollupList.equals(that.rollupList)) {
            return false;
        }
        if (!cubeList.equals(that.cubeList)) {
            return false;
        }
        return groupingSetsSpecification.equals(that.groupingSetsSpecification);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("ordinaryGroupingSet", ordinaryGroupingSet.orElse(null))
                .add("rollupList", rollupList.orElse(null))
                .add("cubeList", cubeList.orElse(null))
                .add("groupingSetsSpecification", groupingSetsSpecification.orElse(null))
                .add("isEmptyGroupingSet", isEmptyGroupingSet)
                .toString();
    }

    @Override
    public int hashCode()
    {
        int result = ordinaryGroupingSet.hashCode();
        result = 31 * result + rollupList.hashCode();
        result = 31 * result + cubeList.hashCode();
        result = 31 * result + groupingSetsSpecification.hashCode();
        result = 31 * result + (isEmptyGroupingSet ? 1 : 0);
        return result;
    }
}
