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
package com.facebook.presto.tpcds;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.DecimalParseResult;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.teradata.tpcds.Results;
import com.teradata.tpcds.column.Column;
import com.teradata.tpcds.column.ColumnType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.spi.type.Decimals.rescale;
import static com.facebook.presto.tpcds.TpcdsMetadata.getPrestoType;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.util.Objects.requireNonNull;

public class TpcdsRecordSet
        implements RecordSet
{
    private final Results results;
    private final List<Column> columns;

    private final List<Type> columnTypes;

    public TpcdsRecordSet(Results results, List<Column> columns)
    {
        requireNonNull(results, "results is null");

        this.results = results;
        this.columns = ImmutableList.copyOf(columns);
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        for (Column column : columns) {
            columnTypes.add(getPrestoType(column.getType()));
        }
        this.columnTypes = columnTypes.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new TpcdsRecordCursor(results.iterator(), columns);
    }

    public class TpcdsRecordCursor
            implements RecordCursor
    {
        private final Iterator<List<List<String>>> rows;
        private final List<Column> columns;
        private List<String> row;
        private boolean closed;

        public TpcdsRecordCursor(Iterator<List<List<String>>> rows, List<Column> columns)
        {
            this.rows = requireNonNull(rows);
            this.columns = requireNonNull(columns);
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
            return getPrestoType(columns.get(field).getType());
        }

        @Override
        public boolean advanceNextPosition()
        {
            if (closed || !rows.hasNext()) {
                closed = true;
                row = null;
                return false;
            }

            row = rows.next().get(0);
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
            Column column = columns.get(field);
            if (column.getType().getBase() == ColumnType.Base.DATE) {
                return LocalDate.parse(row.get(column.getPosition())).toEpochDay();
            }
            if (column.getType().getBase() == ColumnType.Base.TIME) {
                return LocalTime.parse(row.get(column.getPosition())).toNanoOfDay() / 1000000;
            }
            if (column.getType().getBase() == ColumnType.Base.INTEGER) {
                return parseInt(row.get(column.getPosition()));
            }
            if (column.getType().getBase() == ColumnType.Base.DECIMAL) {
                DecimalParseResult decimalParseResult = Decimals.parse(row.get(column.getPosition()));
                return rescale((Long) decimalParseResult.getObject(), decimalParseResult.getType().getScale(), ((DecimalType) columnTypes.get(field)).getScale());
            }
            return parseLong(row.get(column.getPosition()));
        }

        @Override
        public double getDouble(int field)
        {
            checkState(row != null, "No current row");
            return parseDouble(row.get(columns.get(field).getPosition()));
        }

        @Override
        public Slice getSlice(int field)
        {
            checkState(row != null, "No current row");
            Column column = columns.get(field);
            if (column.getType().getBase() == ColumnType.Base.DECIMAL) {
                return (Slice) Decimals.parse(row.get(column.getPosition())).getObject();
            }
            return Slices.utf8Slice(row.get(columns.get(field).getPosition()));
        }

        @Override
        public Object getObject(int field)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isNull(int field)
        {
            checkState(row != null, "No current row");
            return row.get(columns.get(field).getPosition()) == null;
        }

        @Override
        public void close()
        {
            row = null;
            closed = true;
        }
    }
}
