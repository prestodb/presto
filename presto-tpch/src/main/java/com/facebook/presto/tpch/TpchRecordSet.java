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
package com.facebook.presto.tpch;

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;

public class TpchRecordSet<E extends TpchEntity>
        implements RecordSet
{
    public static <E extends TpchEntity> TpchRecordSet<E> createTpchRecordSet(TpchTable<E> table, double scaleFactor)
    {
        return createTpchRecordSet(table, table.getColumns(), scaleFactor, 1, 1);
    }

    public static <E extends TpchEntity> TpchRecordSet<E> createTpchRecordSet(
            TpchTable<E> table,
            Iterable<TpchColumn<E>> columns,
            double scaleFactor,
            int part,
            int partCount)
    {
        return new TpchRecordSet<>(table.createGenerator(scaleFactor, part, partCount), columns);
    }

    private final Iterable<E> table;
    private final List<TpchColumn<E>> columns;
    private final List<ColumnType> columnTypes;

    public TpchRecordSet(Iterable<E> table, Iterable<TpchColumn<E>> columns)
    {
        Preconditions.checkNotNull(table, "readerSupplier is null");

        this.table = table;
        this.columns = ImmutableList.copyOf(columns);

        this.columnTypes = ImmutableList.copyOf(transform(columns, columnTypeGetter()));
    }

    @Override
    public List<ColumnType> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new TpchRecordCursor<>(table.iterator(), columns);
    }

    public class TpchRecordCursor<E extends TpchEntity>
            implements RecordCursor
    {
        private final Iterator<E> rows;
        private final List<TpchColumn<E>> columns;
        private E row;
        private boolean closed;

        public TpchRecordCursor(Iterator<E> rows, List<TpchColumn<E>> columns)
        {
            this.rows = rows;
            this.columns = columns;
        }

        @Override
        public long getTotalBytes()
        {
            return 0;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public ColumnType getType(int field)
        {
            return ColumnType.fromNativeType(getTpchColumn(field).getType());
        }

        @Override
        public boolean advanceNextPosition()
        {
            if (closed || !rows.hasNext()) {
                closed = true;
                row = null;
                return false;
            }

            row = rows.next();
            return true;
        }

        @Override
        public boolean getBoolean(int field)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLong(int field)
        {
            checkState(row != null, "No current row");
            return getTpchColumn(field).getLong(row);
        }

        @Override
        public double getDouble(int field)
        {
            checkState(row != null, "No current row");
            return getTpchColumn(field).getDouble(row);
        }

        @Override
        public byte[] getString(int field)
        {
            checkState(row != null, "No current row");
            return getTpchColumn(field).getString(row).getBytes(Charsets.UTF_8);
        }

        @Override
        public boolean isNull(int field)
        {
            return false;
        }

        @Override
        public void close()
        {
            row = null;
            closed = true;
        }

        private TpchColumn<E> getTpchColumn(int field)
        {
            return columns.get(field);
        }
    }

    public static Function<TpchColumn<?>, ColumnType> columnTypeGetter()
    {
        return new Function<TpchColumn<?>, ColumnType>()
        {
            @Override
            public ColumnType apply(TpchColumn<?> columnMetadata)
            {
                return ColumnType.fromNativeType(columnMetadata.getType());
            }
        };
    }
}
