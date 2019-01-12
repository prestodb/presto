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
package io.prestosql.plugin.tpch;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchColumnType;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.plugin.tpch.TpchMetadata.getPrestoType;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TpchRecordSet<E extends TpchEntity>
        implements RecordSet
{
    public static <E extends TpchEntity> TpchRecordSet<E> createTpchRecordSet(TpchTable<E> table, double scaleFactor)
    {
        return createTpchRecordSet(table, table.getColumns(), scaleFactor, 1, 1, TupleDomain.all());
    }

    public static <E extends TpchEntity> TpchRecordSet<E> createTpchRecordSet(
            TpchTable<E> table,
            List<TpchColumn<E>> columns,
            double scaleFactor,
            int part,
            int partCount,
            TupleDomain<ColumnHandle> predicate)
    {
        return new TpchRecordSet<>(table.createGenerator(scaleFactor, part, partCount), table, columns, predicate);
    }

    private final Iterable<E> rows;
    private final TpchTable<E> table;
    private final List<TpchColumn<E>> columns;
    private final List<Type> columnTypes;
    private final TupleDomain<ColumnHandle> predicate;

    public TpchRecordSet(Iterable<E> rows, TpchTable<E> table, List<TpchColumn<E>> columns, TupleDomain<ColumnHandle> predicate)
    {
        this.rows = requireNonNull(rows, "rows is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.columnTypes = columns.stream()
                .map(TpchMetadata::getPrestoType)
                .collect(toImmutableList());
        this.predicate = requireNonNull(predicate, "predicate is null");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new TpchRecordCursor<>(rows.iterator(), table, columns, predicate);
    }

    public static final class TpchRecordCursor<E extends TpchEntity>
            implements RecordCursor
    {
        private final Iterator<E> rows;
        private final TpchTable<E> table;
        private final List<TpchColumn<E>> columns;
        private final TupleDomain<ColumnHandle> predicate;
        private E row;
        private boolean closed;

        public TpchRecordCursor(Iterator<E> rows, TpchTable<E> table, List<TpchColumn<E>> columns, TupleDomain<ColumnHandle> predicate)
        {
            this.rows = requireNonNull(rows, "rows is null");
            this.table = requireNonNull(table, "table is null");
            this.columns = requireNonNull(columns, "columns is null");
            this.predicate = requireNonNull(predicate, "predicate is null");
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
            return getPrestoType(getTpchColumn(field));
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
            return getLong(getTpchColumn(field));
        }

        private long getLong(TpchColumn<E> tpchColumn)
        {
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
            return getDouble(getTpchColumn(field));
        }

        private double getDouble(TpchColumn<E> tpchColumn)
        {
            return tpchColumn.getDouble(row);
        }

        @Override
        public Slice getSlice(int field)
        {
            checkState(row != null, "No current row");
            return getSlice(getTpchColumn(field));
        }

        private Slice getSlice(TpchColumn<E> tpchColumn)
        {
            return Slices.utf8Slice(tpchColumn.getString(row));
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
            if (predicate.isAll()) {
                return true;
            }
            if (predicate.isNone()) {
                return false;
            }

            Map<ColumnHandle, NullableValue> rowMap = predicate.getDomains().get().keySet().stream()
                    .collect(toImmutableMap(
                            column -> column,
                            column -> {
                                TpchColumnHandle tpchColumnHandle = (TpchColumnHandle) column;
                                Type type = tpchColumnHandle.getType();
                                TpchColumn tpchColumn = table.getColumn(tpchColumnHandle.getColumnName());
                                return NullableValue.of(type, getPrestoObject(tpchColumn, type));
                            }));

            TupleDomain<ColumnHandle> rowTupleDomain = TupleDomain.fromFixedValues(rowMap);

            return predicate.contains(rowTupleDomain);
        }

        private Object getPrestoObject(TpchColumn<E> column, Type type)
        {
            if (type.getJavaType() == long.class) {
                return getLong(column);
            }
            else if (type.getJavaType() == double.class) {
                return getDouble(column);
            }
            else if (type.getJavaType() == Slice.class) {
                return getSlice(column);
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
