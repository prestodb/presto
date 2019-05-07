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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class PlannerUtils
{
    private PlannerUtils() {}

    public static SortOrder toSortOrder(SortItem sortItem)
    {
        if (sortItem.getOrdering() == SortItem.Ordering.ASCENDING) {
            if (sortItem.getNullOrdering() == SortItem.NullOrdering.FIRST) {
                return SortOrder.ASC_NULLS_FIRST;
            }
            return SortOrder.ASC_NULLS_LAST;
        }
        if (sortItem.getNullOrdering() == SortItem.NullOrdering.FIRST) {
            return SortOrder.DESC_NULLS_FIRST;
        }
        return SortOrder.DESC_NULLS_LAST;
    }

    public static OrderingScheme toOrderingScheme(List<SortItem> sortItems, TypeProvider typeProvider)
    {
        // The logic is similar to QueryPlanner::sort
        Map<VariableReferenceExpression, SortOrder> orderings = new LinkedHashMap<>();
        for (SortItem item : sortItems) {
            Expression sortKey = item.getSortKey();
            checkArgument(sortKey instanceof SymbolReference, "must be symbol reference");
            Symbol symbol = Symbol.from(sortKey);
            VariableReferenceExpression variable = new VariableReferenceExpression(symbol.getName(), typeProvider.get(symbol));
            // don't override existing keys, i.e. when "ORDER BY a ASC, a DESC" is specified
            orderings.putIfAbsent(variable, toSortOrder(item));
        }
        return new OrderingScheme(ImmutableList.copyOf(orderings.keySet()), orderings);
    }
}
