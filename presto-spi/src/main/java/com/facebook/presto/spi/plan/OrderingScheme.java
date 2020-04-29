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
package com.facebook.presto.spi.plan;

import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class OrderingScheme
{
    private final List<Ordering> orderBy;
    private final Map<VariableReferenceExpression, SortOrder> orderings;

    @JsonCreator
    public OrderingScheme(@JsonProperty("orderBy") List<Ordering> orderBy)
    {
        requireNonNull(orderBy, "orderBy is null");
        checkArgument(!orderBy.isEmpty(), "orderBy is empty");
        this.orderBy = immutableListCopyOf(orderBy);
        this.orderings = immutableMapCopyOf(orderBy.stream().collect(toMap(Ordering::getVariable, Ordering::getSortOrder)));
    }

    @JsonProperty
    public List<Ordering> getOrderBy()
    {
        return orderBy;
    }

    public List<VariableReferenceExpression> getOrderByVariables()
    {
        return orderBy.stream().map(Ordering::getVariable).collect(toList());
    }

    /**
     *  Sort order of ORDER BY variables.
     */
    public Map<VariableReferenceExpression, SortOrder> getOrderingsMap()
    {
        return orderings;
    }

    public SortOrder getOrdering(VariableReferenceExpression variable)
    {
        checkArgument(orderings.containsKey(variable), "No ordering for variable: %s", variable);
        return orderings.get(variable);
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
        OrderingScheme that = (OrderingScheme) o;
        return Objects.equals(orderBy, that.orderBy) &&
                Objects.equals(orderings, that.orderings);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(orderBy, orderings);
    }

    @Override
    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder(this.getClass().getSimpleName());
        stringBuilder.append(" {");
        stringBuilder.append("orderBy='").append(orderBy).append('\'');
        stringBuilder.append(", orderings='").append(orderings).append('\'');
        stringBuilder.append('}');
        return stringBuilder.toString();
    }

    private static void checkArgument(boolean condition, String messageFormat, Object... args)
    {
        if (!condition) {
            throw new IllegalArgumentException(format(messageFormat, args));
        }
    }

    private static <T> Set<T> immutableSetCopyOf(Collection<T> collection)
    {
        return unmodifiableSet(new TreeSet<>(requireNonNull(collection, "collection is null")));
    }

    private static <T> List<T> immutableListCopyOf(Collection<T> collection)
    {
        return unmodifiableList(new ArrayList<>(requireNonNull(collection, "collection is null")));
    }

    private static <T, H> Map<T, H> immutableMapCopyOf(Map<T, H> map)
    {
        return unmodifiableMap(new TreeMap<>(requireNonNull(map, "map is null")));
    }
}
