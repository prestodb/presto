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
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.Split;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class SampledTpchRecordSetProvider
        extends TpchRecordSetProvider
{
    private final TpchMetadata metadata;

    public SampledTpchRecordSetProvider(String connectorId)
    {
        super(connectorId);
        this.metadata = new TpchMetadata(connectorId);
    }

    @Override
    public RecordSet getRecordSet(Split split, List<? extends ColumnHandle> columns)
    {
        int sampleWeightField = -1;
        for (int i = 0; i < columns.size(); i++) {
            ColumnHandle column = columns.get(i);
            if (column instanceof TpchColumnHandle && ((TpchColumnHandle) column).getColumnName().equals(SampledTpchMetadata.SAMPLE_WEIGHT_COLUMN_NAME)) {
                sampleWeightField = i;
                break;
            }
        }
        List<? extends ColumnHandle> delegatedColumns = new ArrayList<>(columns);
        if (sampleWeightField > -1) {
            delegatedColumns.remove(sampleWeightField);
            RecordSet recordSet;
            if (delegatedColumns.isEmpty()) {
                // Pick a random column, so that we can figure out how many rows there are
                TpchSplit tpchSplit = (TpchSplit) split;
                ColumnHandle column = Iterables.getFirst(metadata.getColumnHandles(tpchSplit.getTableHandle()).values(), null);
                checkNotNull(column, "Could not find any columns");
                recordSet = new EmptyRecordSet(super.getRecordSet(split, ImmutableList.of(column)));
            }
            else {
                recordSet = super.getRecordSet(split, delegatedColumns);
            }
            return new SampledTpchRecordSet(recordSet, sampleWeightField);
        }
        else {
            return super.getRecordSet(split, columns);
        }
    }

    private static class EmptyRecordSet
            implements RecordSet
    {
        private final RecordSet delegate;

        EmptyRecordSet(RecordSet delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public List<ColumnType> getColumnTypes()
        {
            return ImmutableList.of();
        }

        @Override
        public RecordCursor cursor()
        {
            return new EmptyRecordCursor(delegate.cursor());
        }
    }

    private static class EmptyRecordCursor
            implements RecordCursor
    {
        private final RecordCursor delegate;

        EmptyRecordCursor(RecordCursor delegate)
        {
            this.delegate = delegate;
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
            throw new RuntimeException("record cursor is empty");
        }

        @Override
        public boolean advanceNextPosition()
        {
            return delegate.advanceNextPosition();
        }

        @Override
        public boolean getBoolean(int field)
        {
            throw new RuntimeException("record cursor is empty");
        }

        @Override
        public long getLong(int field)
        {
            throw new RuntimeException("record cursor is empty");
        }

        @Override
        public double getDouble(int field)
        {
            throw new RuntimeException("record cursor is empty");
        }

        @Override
        public byte[] getString(int field)
        {
            throw new RuntimeException("record cursor is empty");
        }

        @Override
        public boolean isNull(int field)
        {
            throw new RuntimeException("record cursor is empty");
        }

        @Override
        public void close()
        {
        }
    }

    private static class SampledTpchRecordSet
            implements RecordSet
    {
        private final RecordSet delegate;
        private final int sampleWeightField;

        SampledTpchRecordSet(RecordSet delegate, int sampleWeightField)
        {
            this.delegate = delegate;
            this.sampleWeightField = sampleWeightField;
        }

        @Override
        public List<ColumnType> getColumnTypes()
        {
            List<ColumnType> types = new ArrayList<>();
            types.addAll(delegate.getColumnTypes());
            types.add(sampleWeightField, ColumnType.LONG);
            return ImmutableList.copyOf(types);
        }

        @Override
        public RecordCursor cursor()
        {
            return new SampledTpchRecordCursor(delegate.cursor(), sampleWeightField);
        }
    }

    private static class SampledTpchRecordCursor
            implements RecordCursor
    {
        private final RecordCursor delegate;
        private final int sampleWeightField;

        public SampledTpchRecordCursor(RecordCursor delegate, int sampleWeightField)
        {
            this.delegate = delegate;
            this.sampleWeightField = sampleWeightField;
        }

        @Override
        public boolean isNull(int field)
        {
            if (field == sampleWeightField) {
                return false;
            }
            else {
                return delegate.isNull(field);
            }
        }

        @Override
        public void close()
        {
            delegate.close();
        }

        @Override
        public long getLong(int field)
        {
            if (field == sampleWeightField) {
                return 2;
            }
            else {
                return delegate.getLong(field);
            }
        }

        @Override
        public double getDouble(int field)
        {
            return delegate.getDouble(field);
        }

        @Override
        public byte[] getString(int field)
        {
            return delegate.getString(field);
        }

        @Override
        public long getTotalBytes()
        {
            return delegate.getTotalBytes();
        }

        @Override
        public long getCompletedBytes()
        {
            return delegate.getCompletedBytes();
        }

        @Override
        public ColumnType getType(int field)
        {
            if (field == sampleWeightField) {
                return ColumnType.LONG;
            }
            else {
                return delegate.getType(field);
            }
        }

        @Override
        public boolean advanceNextPosition()
        {
            return delegate.advanceNextPosition();
        }

        @Override
        public boolean getBoolean(int field)
        {
            return delegate.getBoolean(field);
        }
    }
}
