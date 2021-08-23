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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.common.PrestoException;
import com.facebook.presto.common.block.SortOrder;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.common.block.SortOrder.DESC_NULLS_LAST;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.metastore.SortingColumn.Order.ASCENDING;
import static com.facebook.presto.hive.metastore.SortingColumn.Order.DESCENDING;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

@Immutable
public class SortingColumn
{
    public enum Order
    {
        ASCENDING(ASC_NULLS_FIRST, 1),
        DESCENDING(DESC_NULLS_LAST, 0);

        private final SortOrder sortOrder;
        private final int hiveOrder;

        Order(SortOrder sortOrder, int hiveOrder)
        {
            this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
            this.hiveOrder = hiveOrder;
        }

        public SortOrder getSortOrder()
        {
            return sortOrder;
        }

        public int getHiveOrder()
        {
            return hiveOrder;
        }

        public static Order fromMetastoreApiOrder(int value, String tablePartitionName)
        {
            for (Order order : values()) {
                if (value == order.getHiveOrder()) {
                    return order;
                }
            }
            throw new PrestoException(HIVE_INVALID_METADATA, "Table/partition metadata has invalid sorting order: " + tablePartitionName);
        }
    }

    private final String columnName;
    private final Order order;

    @JsonCreator
    public SortingColumn(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("order") Order order)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.order = requireNonNull(order, "order is null");
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Order getOrder()
    {
        return order;
    }

    public static SortingColumn fromMetastoreApiOrder(org.apache.hadoop.hive.metastore.api.Order order, String tablePartitionName)
    {
        return new SortingColumn(order.getCol(), Order.fromMetastoreApiOrder(order.getOrder(), tablePartitionName));
    }

    public static SortingColumn sortingColumnFromString(String name)
    {
        SortingColumn.Order order = ASCENDING;
        String upper = name.toUpperCase(ENGLISH);
        if (upper.endsWith(" ASC")) {
            name = name.substring(0, name.length() - 4).trim();
        }
        else if (upper.endsWith(" DESC")) {
            name = name.substring(0, name.length() - 5).trim();
            order = DESCENDING;
        }
        return new SortingColumn(name, order);
    }

    public static String sortingColumnToString(SortingColumn column)
    {
        return column.getColumnName() + ((column.getOrder() == DESCENDING) ? " DESC" : "");
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("order", order)
                .toString();
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

        SortingColumn that = (SortingColumn) o;
        return Objects.equals(columnName, that.columnName) &&
                order == that.order;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, order);
    }
}
