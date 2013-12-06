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

import com.facebook.presto.operator.SortOrder;
import com.google.common.collect.ImmutableList;

import java.util.Comparator;
import java.util.List;

import static com.facebook.presto.tuple.TupleInfo.Type;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Compares two rows element by element.
 */
public class FieldOrderedTupleComparator
        implements Comparator<TupleReadable[]>
{
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrders;

    public FieldOrderedTupleComparator(List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        checkNotNull(sortChannels, "sortChannels is null");
        checkNotNull(sortOrders, "sortOrders is null");
        checkArgument(sortChannels.size() == sortOrders.size(), "sortFields size (%s) doesn't match sortOrders size (%s)", sortChannels.size(), sortOrders.size());

        this.sortChannels = ImmutableList.copyOf(sortChannels);
        this.sortOrders = ImmutableList.copyOf(sortOrders);
    }

    @Override
    public int compare(TupleReadable[] leftRow, TupleReadable[] rightRow)
    {
        for (int index = 0; index < sortChannels.size(); index++) {
            int channel = sortChannels.get(index);
            SortOrder sortOrder = sortOrders.get(index);

            TupleReadable left = leftRow[channel];
            TupleReadable right = rightRow[channel];

            boolean leftIsNull = left.isNull(0);
            boolean rightIsNull = right.isNull(0);

            if (leftIsNull && rightIsNull) {
                return 0;
            }

            if (leftIsNull) {
                return sortOrder.isNullsFirst() ? -1 : 1;
            }

            if (rightIsNull) {
                return sortOrder.isNullsFirst() ? 1 : -1;
            }

            Type type = left.getTupleInfo().getType();
            int comparison;
            switch (type) {
                case BOOLEAN:
                    comparison = Boolean.compare(left.getBoolean(0), right.getBoolean(0));
                    break;
                case FIXED_INT_64:
                    comparison = Long.compare(left.getLong(0), right.getLong(0));
                    break;
                case DOUBLE:
                    comparison = Double.compare(left.getDouble(0), right.getDouble(0));
                    break;
                case VARIABLE_BINARY:
                    comparison = left.getSlice(0).compareTo(right.getSlice(0));
                    break;
                default:
                    throw new AssertionError("unimplemented type: " + type);
            }

            if (comparison != 0) {
                return sortOrder.isAscending() ? comparison : -comparison;
            }
        }
        return 0;
    }
}
