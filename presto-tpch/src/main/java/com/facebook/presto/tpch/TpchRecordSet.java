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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchColumnType;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.tpch.TpchMetadata.getPrestoType;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class TpchRecordSet<E extends TpchEntity>
        implements RecordSet
{
    public static <E extends TpchEntity> TpchRecordSet<E> createTpchRecordSet(TpchTable<E> table, double scaleFactor)
    {
        return createTpchRecordSet(table, table.getColumns(), scaleFactor, 1, 1, Optional.empty());
    }

    public static <E extends TpchEntity> TpchRecordSet<E> createTpchRecordSet(
            TpchTable<E> table,
            Iterable<TpchColumn<E>> columns,
            double scaleFactor,
            int part,
            int partCount,
            Optional<TupleDomain<ColumnHandle>> predicate)
    {
        return new TpchRecordSet<>(table.createGenerator(scaleFactor, part, partCount), columns, predicate);
    }

    private final Iterable<E> table;
    private final List<TpchColumn<E>> columns;
    private final List<Type> columnTypes;
    private final List<TpchColumnHandle> columnHandles;
    private final Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate;

    public TpchRecordSet(Iterable<E> table, Iterable<TpchColumn<E>> columns, Optional<TupleDomain<ColumnHandle>> predicate)
    {
        requireNonNull(table, "readerSupplier is null");

        this.table = table;
        this.columns = ImmutableList.copyOf(columns);

        this.columnTypes = ImmutableList.copyOf(transform(columns, column -> getPrestoType(column.getType())));

        columnHandles = this.columns.stream()
                .map(column -> new TpchColumnHandle(column.getColumnName(), getPrestoType(column.getType())))
                .collect(toList());
        this.predicate = predicate.map(TpchRecordSet::convertToPredicate);
    }

    static Predicate<Map<ColumnHandle, NullableValue>> convertToPredicate(TupleDomain<ColumnHandle> tupleDomain)
    {
        return bindings -> tupleDomain.contains(TupleDomain.fromFixedValues(bindings));
    }

    @Override
    public List<Type> getColumnTypes()
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
        public Type getType(int field)
        {
            return getPrestoType(getTpchColumn(field).getType());
        }

        @Override
        public boolean advanceNextPosition()
        {
            while (!closed && rows.hasNext()) {
                row = rows.next();
                if (rowMatchesPredicate()) {
                    return true;
                }
            }

            closed = true;
            row = null;
            return false;
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
            TpchColumn<E> tpchColumn = getTpchColumn(field);
            if (tpchColumn.getType().getBase() == TpchColumnType.Base.DATE) {
                return tpchColumn.getDate(row);
            }
            if (tpchColumn.getType().getBase() == TpchColumnType.Base.INTEGER) {
                return tpchColumn.getInteger(row);
            }
            return tpchColumn.getIdentifier(row);
        }

        @Override
        public double getDouble(int field)
        {
            checkState(row != null, "No current row");
            return getTpchColumn(field).getDouble(row);
        }

        @Override
        public Slice getSlice(int field)
        {
            checkState(row != null, "No current row");
            return Slices.utf8Slice(getTpchColumn(field).getString(row));
        }

        @Override
        public Object getObject(int field)
        {
            throw new UnsupportedOperationException();
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

        private boolean rowMatchesPredicate()
        {
            if (!predicate.isPresent()) {
                return true;
            }
            return predicate.get().test(rowMap());
        }

        private Map<ColumnHandle, NullableValue> rowMap()
        {
            ImmutableMap.Builder<ColumnHandle, NullableValue> builder = ImmutableMap.builder();
            for (int field = 0; field < columnHandles.size(); ++field) {
                Type type = columnTypes.get(field);
                builder.put(columnHandles.get(field), NullableValue.of(type, getPrestoObject(field, type)));
            }
            return builder.build();
        }

        private Object getPrestoObject(int field, Type type)
        {
            if (type.getJavaType() == long.class) {
                return getLong(field);
            }
            else if (type.getJavaType() == double.class) {
                return getDouble(field);
            }
            else if (type.getJavaType() == Slice.class) {
                return getSlice(field);
            }
            else {
                throw new PrestoException(NOT_SUPPORTED, format("Unsupported column type %s", type.getDisplayName()));
            }
        }

        private TpchColumn<E> getTpchColumn(int field)
        {
            return columns.get(field);
        }
    }
}
