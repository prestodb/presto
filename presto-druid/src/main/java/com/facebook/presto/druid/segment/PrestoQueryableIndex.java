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
package com.facebook.presto.druid.segment;

import com.google.common.base.Supplier;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.query.OrderBy;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.BaseColumnHolder;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.Indexed;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PrestoQueryableIndex
        implements QueryableIndex
{
    private final Interval dataInterval;
    private final Indexed<String> availableDimensions;
    private final Map<String, Supplier<ColumnHolder>> columns;
    @Nullable
    private final Metadata metadata;
    @Nullable
    private final BitmapFactory bitmapFactory;

    public PrestoQueryableIndex(
            Interval dataInterval,
            Indexed<String> availableDimensions,
            Map<String, Supplier<ColumnHolder>> columns,
            @Nullable Metadata metadata,
            @Nullable BitmapFactory bitmapFactory)
    {
        this.dataInterval = dataInterval;
        this.availableDimensions = availableDimensions;
        this.columns = columns;
        this.metadata = metadata;
        this.bitmapFactory = bitmapFactory;
    }

    @Override
    public Interval getDataInterval()
    {
        return dataInterval;
    }

    @Override
    public int getNumRows()
    {
        return 0;
    }

    @Override
    public Indexed<String> getAvailableDimensions()
    {
        return availableDimensions;
    }

    @Override
    public List<String> getColumnNames()
    {
        return new ArrayList<>(columns.keySet());
    }

    @Nullable
    @Override
    public BaseColumnHolder getColumnHolder(String columnName)
    {
        Supplier<ColumnHolder> supplier = columns.get(columnName);
        if (supplier == null) {
            return null;
        }
        ColumnHolder holder = supplier.get();
        return (BaseColumnHolder) holder;
    }

    @Nullable
    @Override
    public Metadata getMetadata()
    {
        return metadata;
    }

    @Override
    public List<OrderBy> getOrdering()
    {
        return Collections.emptyList();
    }

    @Override
    public Map<String, org.apache.druid.segment.DimensionHandler> getDimensionHandlers()
    {
        return Collections.emptyMap();
    }

    @Nullable
    @Override
    public BitmapFactory getBitmapFactoryForDimensions()
    {
        return bitmapFactory;
    }

    @Override
    public void close()
    {
        // No-op
    }
}
