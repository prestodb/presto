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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class MappedRecordSet
        implements RecordSet
{
    private final RecordSet delegate;
    private final int[] delegateFieldIndex;
    private final List<Type> columnTypes;

    public MappedRecordSet(RecordSet delegate, List<Integer> delegateFieldIndex)
    {
        this.delegate = delegate;
        this.delegateFieldIndex = new int[delegateFieldIndex.size()];
        for (int i = 0; i < delegateFieldIndex.size(); i++) {
            this.delegateFieldIndex[i] = delegateFieldIndex.get(i);
        }

        List<Type> delegateColumnTypes = delegate.getColumnTypes();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        for (int delegateField : delegateFieldIndex) {
            checkArgument(delegateField >= 0 && delegateField < delegateColumnTypes.size(), "Invalid system field %s", delegateField);
            columnTypes.add(delegateColumnTypes.get(delegateField));
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
        return new MappedRecordCursor(delegate.cursor(), delegateFieldIndex);
    }

    private static class MappedRecordCursor
            implements RecordCursor
    {
        private final RecordCursor delegate;
        private final int[] delegateFieldIndex;

        private MappedRecordCursor(RecordCursor delegate, int[] delegateFieldIndex)
        {
            this.delegate = delegate;
            this.delegateFieldIndex = delegateFieldIndex;
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
        public long getReadTimeNanos()
        {
            return delegate.getReadTimeNanos();
        }

        @Override
        public Type getType(int field)
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
            return delegate.getBoolean(toDelegateField(field));
        }

        @Override
        public long getLong(int field)
        {
            return delegate.getLong(toDelegateField(field));
        }

        @Override
        public double getDouble(int field)
        {
            return delegate.getDouble(toDelegateField(field));
        }

        @Override
        public byte[] getString(int field)
        {
            return delegate.getString(toDelegateField(field));
        }

        @Override
        public boolean isNull(int field)
        {
            return delegate.isNull(toDelegateField(field));
        }

        @Override
        public void close()
        {
            delegate.close();
        }

        private int toDelegateField(int field)
        {
            checkArgument(field >= 0, "field is negative");
            checkArgument(field < delegateFieldIndex.length);
            return delegateFieldIndex[field];
        }
    }
}
