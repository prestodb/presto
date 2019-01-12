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
package io.prestosql.split;

import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MappedRecordSet
        implements RecordSet
{
    private final RecordSet delegate;
    private final int[] delegateFieldIndex;
    private final List<Type> columnTypes;

    public MappedRecordSet(RecordSet delegate, List<Integer> delegateFieldIndex)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.delegateFieldIndex = Ints.toArray(requireNonNull(delegateFieldIndex, "delegateFieldIndex is null"));

        List<Type> types = delegate.getColumnTypes();
        this.columnTypes = delegateFieldIndex.stream().map(types::get).collect(toImmutableList());
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
            return delegate.getType(toDelegateField(field));
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
        public Slice getSlice(int field)
        {
            return delegate.getSlice(toDelegateField(field));
        }

        @Override
        public Object getObject(int field)
        {
            return delegate.getObject(toDelegateField(field));
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
            checkElementIndex(field, delegateFieldIndex.length, "field");
            return delegateFieldIndex[field];
        }
    }
}
