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
package com.facebook.presto.split;

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class MappedRecordSet
        implements RecordSet
{
    private final RecordSet delegate;
    private final List<Integer> userToSystemFieldIndex;
    private final List<ColumnType> columnTypes;

    public MappedRecordSet(RecordSet delegate, List<Integer> userToSystemFieldIndex)
    {
        this.delegate = delegate;
        this.userToSystemFieldIndex = userToSystemFieldIndex;

        List<ColumnType> delegateColumnTypes = delegate.getColumnTypes();
        ImmutableList.Builder<ColumnType> columnTypes = ImmutableList.builder();
        for (int systemField : userToSystemFieldIndex) {
            checkArgument(systemField >= 0 && systemField < delegateColumnTypes.size(), "Invalid system field %s", systemField);
            columnTypes.add(delegateColumnTypes.get(systemField));
        }
        this.columnTypes = columnTypes.build();
    }

    @Override
    public List<ColumnType> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new MappedRecordCursor(delegate.cursor(), userToSystemFieldIndex);
    }

    private static class MappedRecordCursor
            implements RecordCursor
    {
        private final RecordCursor delegate;
        private final List<Integer> userToSystemFieldIndex;

        private MappedRecordCursor(RecordCursor delegate, List<Integer> userToSystemFieldIndex)
        {
            this.delegate = delegate;
            this.userToSystemFieldIndex = ImmutableList.copyOf(userToSystemFieldIndex);
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
            return delegate.getType(field);
        }

        @Override
        public boolean advanceNextPosition()
        {
            return delegate.advanceNextPosition();
        }

        @Override
        public boolean getBoolean(int field)
        {
            return delegate.getBoolean(userFieldToSystemField(field));
        }

        @Override
        public long getLong(int field)
        {
            return delegate.getLong(userFieldToSystemField(field));
        }

        @Override
        public double getDouble(int field)
        {
            return delegate.getDouble(userFieldToSystemField(field));
        }

        @Override
        public byte[] getString(int field)
        {
            return delegate.getString(userFieldToSystemField(field));
        }

        @Override
        public boolean isNull(int field)
        {
            return delegate.isNull(userFieldToSystemField(field));
        }

        @Override
        public void close()
        {
            delegate.close();
        }

        private int userFieldToSystemField(int field)
        {
            Preconditions.checkArgument(field >= 0, "field is negative");
            checkArgument(field < userToSystemFieldIndex.size());
            return userToSystemFieldIndex.get(field);
        }
    }
}
