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
import com.google.common.collect.ImmutableList;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndex;

public class AppendingRecordSet
        implements RecordSet
{
    private final RecordSet delegate;
    private final List<Object> appendedValues;
    private final List<ColumnType> appendedTypes;

    public AppendingRecordSet(RecordSet delegate, List<Object> appendedValues, List<ColumnType> appendedTypes)
    {
        this.delegate = checkNotNull(delegate, "delegate is null");
        this.appendedValues = new ArrayList<>(checkNotNull(appendedValues, "appendedValues is null")); // May contain null elements
        this.appendedTypes = ImmutableList.copyOf(checkNotNull(appendedTypes, "appendedTypes is null"));
        checkArgument(appendedValues.size() == appendedTypes.size(), "appendedValues must have the same size as appendedTypes");
        for (int i = 0; i < appendedValues.size(); i++) {
            Object value = appendedValues.get(i);
            if (value != null) {
                checkArgument(appendedTypes.get(i).getNativeType().isInstance(value), "Object value does not match declared type");
            }
        }
    }

    @Override
    public List<ColumnType> getColumnTypes()
    {
        return ImmutableList.<ColumnType>builder()
                .addAll(delegate.getColumnTypes())
                .addAll(appendedTypes)
                .build();
    }

    @Override
    public RecordCursor cursor()
    {
        return new AppendingRecordCursor(delegate.cursor(), delegate.getColumnTypes().size(), appendedValues, appendedTypes);
    }

    private static class AppendingRecordCursor
            implements RecordCursor
    {
        private final RecordCursor delegate;
        private final int delegateFieldCount;
        private final List<Object> appendedValues;
        private final List<ColumnType> appendedTypes;

        private AppendingRecordCursor(RecordCursor delegate, int delegateFieldCount, List<Object> appendedValues, List<ColumnType> appendedTypes)
        {
            this.delegate = checkNotNull(delegate, "delegate is null");
            this.delegateFieldCount = delegateFieldCount;
            checkArgument(delegateFieldCount >= 0, "delegateFieldCount must be greater than or equal to zero");
            this.appendedValues = checkNotNull(appendedValues, "appendedValues is null"); // May contain null elements
            this.appendedTypes = ImmutableList.copyOf(checkNotNull(appendedTypes, "appendedTypes is null"));
            checkArgument(appendedValues.size() == appendedTypes.size(), "appendedValues must have the same size as appendedTypes");
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
            checkPositionIndex(field, delegateFieldCount + appendedTypes.size());
            if (field < delegateFieldCount) {
                return delegate.getType(field);
            }
            else {
                return appendedTypes.get(field - delegateFieldCount);
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
            checkPositionIndex(field, delegateFieldCount + appendedTypes.size());
            if (field < delegateFieldCount) {
                return delegate.getBoolean(field);
            }
            else {
                return (Boolean) appendedValues.get(field - delegateFieldCount);
            }
        }

        @Override
        public long getLong(int field)
        {
            checkPositionIndex(field, delegateFieldCount + appendedTypes.size());
            if (field < delegateFieldCount) {
                return delegate.getLong(field);
            }
            else {
                return (Long) appendedValues.get(field - delegateFieldCount);
            }
        }

        @Override
        public double getDouble(int field)
        {
            checkPositionIndex(field, delegateFieldCount + appendedTypes.size());
            if (field < delegateFieldCount) {
                return delegate.getDouble(field);
            }
            else {
                return (Double) appendedValues.get(field - delegateFieldCount);
            }
        }

        @Override
        public byte[] getString(int field)
        {
            checkPositionIndex(field, delegateFieldCount + appendedTypes.size());
            if (field < delegateFieldCount) {
                return delegate.getString(field);
            }
            else {
                return ((String) appendedValues.get(field - delegateFieldCount)).getBytes(StandardCharsets.UTF_8);
            }
        }

        @Override
        public boolean isNull(int field)
        {
            checkPositionIndex(field, delegateFieldCount + appendedTypes.size());
            if (field < delegateFieldCount) {
                return delegate.isNull(field);
            }
            else {
                return appendedValues.get(field - delegateFieldCount) == null;
            }
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }
}
