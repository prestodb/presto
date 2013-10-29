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
package com.facebook.presto.util;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closeables;
import com.google.common.io.InputSupplier;
import com.google.common.io.LineReader;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

import static com.facebook.presto.metadata.MetadataUtil.columnTypeGetter;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Iterables.transform;

public class DelimitedRecordSet
        implements RecordSet
{
    private final InputSupplier<? extends Reader> readerSupplier;
    private final Splitter columnSplitter;
    private final List<ColumnMetadata> columns;
    private final List<ColumnType> columnTypes;

    public DelimitedRecordSet(InputSupplier<? extends Reader> readerSupplier, Splitter columnSplitter, ColumnMetadata... columns)
    {
        this(readerSupplier, columnSplitter, ImmutableList.copyOf(columns));
    }

    public DelimitedRecordSet(InputSupplier<? extends Reader> readerSupplier, Splitter columnSplitter, Iterable<ColumnMetadata> columns)
    {
        Preconditions.checkNotNull(readerSupplier, "readerSupplier is null");
        Preconditions.checkNotNull(columnSplitter, "columnSplitter is null");

        this.readerSupplier = readerSupplier;
        this.columnSplitter = columnSplitter;
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
        return new DelimitedRecordCursor(readerSupplier, columnSplitter, columns);
    }

    private static class DelimitedRecordCursor
            implements RecordCursor
    {
        private final Reader reader;
        private final LineReader lineReader;
        private final Splitter columnSplitter;
        private final List<ColumnMetadata> columns;
        private List<String> row;

        private DelimitedRecordCursor(InputSupplier<? extends Reader> readerSupplier, Splitter columnSplitter, List<ColumnMetadata> columns)
        {
            try {
                this.reader = readerSupplier.getInput();
                this.lineReader = new LineReader(reader);
                this.columnSplitter = columnSplitter;
                this.columns = columns;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
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
        public ColumnType getType(int field)
        {
            return columns.get(field).getType();
        }

        @Override
        public boolean advanceNextPosition()
        {
            try {
                String line = lineReader.readLine();
                if (line == null) {
                    row = null;
                    return false;
                }
                row = ImmutableList.copyOf(columnSplitter.split(line));
                return true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public boolean getBoolean(int field)
        {
            return Boolean.parseBoolean(getField(field));
        }

        @Override
        public long getLong(int field)
        {
            return Long.parseLong(getField(field));
        }

        @Override
        public double getDouble(int field)
        {
            return Double.parseDouble(getField(field));
        }

        @Override
        public byte[] getString(int field)
        {
            return getField(field).getBytes(UTF_8);
        }

        @Override
        public boolean isNull(int field)
        {
            return getField(field).isEmpty();
        }

        private String getField(int field)
        {
            ColumnMetadata columnMetadata = columns.get(field);
            return row.get(columnMetadata.getOrdinalPosition());
        }

        @Override
        public void close()
        {
            Closeables.closeQuietly(reader);
        }
    }
}
