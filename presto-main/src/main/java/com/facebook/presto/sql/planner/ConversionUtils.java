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
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class ConversionUtils
{
    private ConversionUtils()
    {
    }

    public static OrderingScheme fromSortItems(List<SortItem> sortItems)
    {
        return fromSortItems(sortItems, e -> {
            checkArgument(e instanceof SymbolReference, "Sort key must be symbol reference");
            return new Symbol(((SymbolReference) e).getName());
        });
    }

    public static OrderingScheme fromSortItems(List<SortItem> sortItems, Function<Expression, Symbol> sortKeyTranslator)
    {
        Map<Symbol, SortOrder> orderings = new LinkedHashMap<>();
        for (SortItem item : sortItems) {
            orderings.putIfAbsent(sortKeyTranslator.apply(item.getSortKey()), toSortOrder(item));
        }
        return new OrderingScheme(orderings.keySet().stream().collect(toImmutableList()), orderings);
    }

    public static SortOrder toSortOrder(SortItem sortItem)
    {
        if (sortItem.getOrdering() == SortItem.Ordering.ASCENDING) {
            if (sortItem.getNullOrdering() == SortItem.NullOrdering.FIRST) {
                return SortOrder.ASC_NULLS_FIRST;
            }
            else {
                return SortOrder.ASC_NULLS_LAST;
            }
        }
        else {
            if (sortItem.getNullOrdering() == SortItem.NullOrdering.FIRST) {
                return SortOrder.DESC_NULLS_FIRST;
            }
            else {
                return SortOrder.DESC_NULLS_LAST;
            }
        }
    }
}
