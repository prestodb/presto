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
package com.facebook.presto.spi.relation;

import com.facebook.presto.spi.relation.column.ColumnExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public final class AggregateExpression
        extends UnaryTableExpression
{
    private final List<ColumnExpression> aggregations;
    private final List<ColumnExpression> groups;
    private final List<List<Integer>> groupSets;
    private final TableExpression source;

    public AggregateExpression(
            List<ColumnExpression> aggregations,
            List<ColumnExpression> groups,
            List<List<Integer>> groupSets,
            TableExpression source)
    {
        this.aggregations = requireNonNull(aggregations, "aggregations is null");
        this.groups = requireNonNull(groups, "groups is null");
        this.groupSets = requireNonNull(groupSets, "groupSets is null");
        this.source = requireNonNull(source, "source is null");
    }

    public List<ColumnExpression> getAggregations()
    {
        return aggregations;
    }

    public List<ColumnExpression> getGroups()
    {
        return groups;
    }

    public List<List<Integer>> getGroupSets()
    {
        return groupSets;
    }

    @Override
    public TableExpression getSource()
    {
        return source;
    }

    @Override
    public List<ColumnExpression> getOutput()
    {
        List<ColumnExpression> outputs = new ArrayList<>(groups.size() + aggregations.size() + groupSets.size() > 1 ? 1 : 0);
        outputs.addAll(groups);
        outputs.addAll(aggregations);
        return unmodifiableList(outputs);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AggregateExpression)) {
            return false;
        }
        AggregateExpression aggregate = (AggregateExpression) o;
        return Objects.equals(aggregations, aggregate.aggregations) &&
                Objects.equals(groups, aggregate.groups) &&
                Objects.equals(source, aggregate.source);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(aggregations, groups, source);
    }

    @Override
    public String toString()
    {
        return "AggregateExpression{" +
                "aggregations=" + aggregations +
                ", groups=" + groups +
                ", groupSets=" + groupSets +
                ", source=" + source +
                '}';
    }
}
