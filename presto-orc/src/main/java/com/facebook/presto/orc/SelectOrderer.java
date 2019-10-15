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
package com.facebook.presto.orc;

import com.facebook.presto.spi.type.AbstractIntType;
import com.facebook.presto.spi.type.AbstractLongType;
import com.facebook.presto.spi.type.AbstractVariableWidthType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

/**
 * Things that extend this interface represent objects
 * that take in SelectOrderables and reorder them to
 * fit different cost and projected selectivity rules.
 * <p>
 * The idea is to read different columns (and filter them)
 * in the most efficient order.
 */
public interface SelectOrderer
{
    public static class OrcSelectOrderer
            implements SelectOrderer
    {
        public final static int UNKNOWN_TYPE_WEIGHT = 100;

        private static int strategy(SelectOrderable orderable)
        {
            if (orderable instanceof TupleDomainFilter) {
                TupleDomainFilter filter = (TupleDomainFilter) orderable;
                if (filter instanceof TupleDomainFilter.BigintRange) {
                    if (((TupleDomainFilter.BigintRange) filter).isSingleValue()) {
                        // Integer equality. Generally cheap.
                        return 10;
                    }
                    return 50;
                }

                if (filter instanceof TupleDomainFilter.BigintValues || filter instanceof TupleDomainFilter.BigintMultiRange) {
                    return 50;
                }

                return 100;
            }
            if (orderable instanceof FilterFunction) {
                FilterFunction filter = (FilterFunction) orderable;
                if (filter.getInputChannels().length == 0) {
                    return 1;
                }
                return 1000;
            }

            if (orderable instanceof OrderableColumn){
                OrderableColumn column = (OrderableColumn) orderable;
                if(column.isConstant()){
                    return 1;
                }
                if(!column.getType().isPresent()){
                    return UNKNOWN_TYPE_WEIGHT;
                }
                Type type = column.getType().get();
                if(type instanceof BooleanType){
                    return 10;
                }
                if(type instanceof AbstractLongType || type instanceof AbstractIntType){
                    return 20;
                }
                if(type instanceof AbstractVariableWidthType){
                    return 50;
                }
                return 100;
            }

            return 100;
            // throw new IllegalStateException("Can only strategy a SelectOrderable of type TypleDomainFilter, FilterFunction, or OrderableColumn");
        }

        /**
         * @param orderables, a list of things wrapped with that interface (TupleDomainFilters, OrderableColumn, FilterFunction) that we want to sort
         * @param orderableToColumns A function that takes in a SelectOrderable and returns the list of columns it connects to
         * @return A list of lists. Each sublist starts with the column associated with it, then the TupleDomainFilters applied to that column,
         *          then the FilterFunctions that should be applied right after reading that column.
         *          The outer list orders the columns (which are the first element of the inner list) from first to last order.
         */
        public static List<List<SelectOrderable>> order(List<SelectOrderable> orderables, Function<SelectOrderable, List<OrderableColumn>> orderableToColumns)
        {
            Map<SelectOrderable, Integer> orderableToScore = orderables
                    .stream()
                    .collect(toImmutableMap(entry -> entry, entry -> strategy(entry)));


            orderableToScore = orderables
                .stream()
                .collect(toImmutableMap(entry -> entry,
                        entry -> strategy(entry) * orderableToColumns.apply(entry).stream()
                            .mapToInt(column -> strategy(column))
                            .max()
                            .orElse(UNKNOWN_TYPE_WEIGHT)));


            List<SelectOrderable> sortedNonColumnOrderables = orderableToScore.entrySet().stream()
                    .filter(entry -> !(entry.getKey() instanceof OrderableColumn))
                    .sorted(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .collect(toImmutableList());

            LinkedList<List<SelectOrderable>> columnOrder = new LinkedList<List<SelectOrderable>>();
            Map<OrderableColumn, Integer> columnLocation = new HashMap<OrderableColumn, Integer>();

            for (SelectOrderable orderable: sortedNonColumnOrderables) {
                int maxIndexOfPossibleColumns = 0;
                for(OrderableColumn column : orderableToColumns.apply(orderable)){
                    if(!columnLocation.containsKey(column)){
                        int newIndex = columnOrder.size();
                        columnOrder.add(new LinkedList<SelectOrderable>());
                        columnOrder.getLast().add(column);
                        columnLocation.put(column, newIndex);
                        maxIndexOfPossibleColumns = newIndex;
                    }
                    else {
                        maxIndexOfPossibleColumns = Integer.max(maxIndexOfPossibleColumns, columnLocation.get(column));
                    }
                }
                if(orderable instanceof TupleDomainFilter){
                    // TupleDomainFilters come after the column, but before FilterFunctions
                    columnOrder.get(maxIndexOfPossibleColumns).add(1, orderable);
                }
                else{
                    columnOrder.get(maxIndexOfPossibleColumns).add(orderable);
                }

            }

            for (SelectOrderable orderable: orderables){
                if(orderable instanceof OrderableColumn && ! columnLocation.containsKey(orderable)){
                    columnOrder.add(ImmutableList.of(orderable));
                }
            }
            return ImmutableList.copyOf(columnOrder);

        }
    }

    /**
     * Things that extend this interface represent objects that need to be
     * ordered in terms of which we read first, when using push down filters.
     * In the ORC world, that means columns, TupleDomainFilters, and FilterFunctions.
     * <p>
     * They are passed to a SelectOrderer
     */
    interface SelectOrderable
    {
    }

    public class OrderableColumn
            implements SelectOrderable
    {
        private final boolean isConstant;
        private final int index;
        private final Optional<Type> type;
        public final static OrderableColumn NULL_COLUMN = OrderableColumn.of(true, -1);

        private OrderableColumn(boolean isConstant, int index, Optional<Type> type)
        {
            this.isConstant = isConstant;
            this.index = index;
            this.type = type;
        }

        public static OrderableColumn of(boolean isConstant, int index)
        {
            return new OrderableColumn(isConstant, index, Optional.empty());
        }

        public static OrderableColumn of(boolean isConstant, int index, Type type)
        {
            return new OrderableColumn(isConstant, index, Optional.of(type));
        }

        public boolean isConstant() {
            return isConstant;
        }

        public int getIndex() {
            return index;
        }

        public Optional<Type> getType(){
            return type;
        }
    }
}


