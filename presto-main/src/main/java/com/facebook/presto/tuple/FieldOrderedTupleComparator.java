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
package com.facebook.presto.tuple;

import com.facebook.presto.sql.tree.SortItem;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Comparator;
import java.util.List;

/**
 * Compares two field-aligned TupleReadables field by field.
 */
public class FieldOrderedTupleComparator
        implements Comparator<TupleReadable>
{
    private final List<Integer> sortFields;
    private final List<SortItem.Ordering> sortOrders;

    public FieldOrderedTupleComparator(List<Integer> sortFields, List<SortItem.Ordering> sortOrders)
    {
        Preconditions.checkNotNull(sortFields, "sortFields is null");
        Preconditions.checkNotNull(sortOrders, "sortOrders is null");
        Preconditions.checkArgument(sortFields.size() == sortOrders.size(), "sortFields size (%s) doesn't match sortOrders size (%s)", sortFields.size(), sortOrders.size());

        this.sortFields = ImmutableList.copyOf(sortFields);
        this.sortOrders = ImmutableList.copyOf(sortOrders);
    }

    @Override
    public int compare(TupleReadable o1, TupleReadable o2)
    {
        List<TupleInfo.Type> types = o1.getTupleInfo().getTypes();

        for (int index = 0; index < sortFields.size(); index++) {
            int field = sortFields.get(index);
            SortItem.Ordering order = sortOrders.get(index);
            TupleInfo.Type type = types.get(field);

            int comparison;
            switch (type) {
                case BOOLEAN:
                    comparison = Boolean.compare(o1.getBoolean(field), o2.getBoolean(field));
                    break;
                case FIXED_INT_64:
                    comparison = Long.compare(o1.getLong(field), o2.getLong(field));
                    break;
                case DOUBLE:
                    comparison = Double.compare(o1.getDouble(field), o2.getDouble(field));
                    break;
                case VARIABLE_BINARY:
                    comparison = o1.getSlice(field).compareTo(o2.getSlice(field));
                    break;
                default:
                    throw new AssertionError("unimplemented type: " + type);
            }

            if (comparison != 0) {
                if (order == SortItem.Ordering.DESCENDING) {
                    return -comparison;
                }

                return comparison;
            }
        }
        return 0;
    }
}
