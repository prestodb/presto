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

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class InfiniteRecordSet
        implements RecordSet
{
    private final List<ColumnType> types;
    private final List<?> record;

    public InfiniteRecordSet(List<ColumnType> types, List<?> record)
    {
        this.types = types;
        this.record = record;
    }

    @Override
    public List<ColumnType> getColumnTypes()
    {
        return types;
    }

    @Override
    public RecordCursor cursor()
    {
        return new InMemoryRecordCursor(types, record);
    }

    private static class InMemoryRecordCursor
            implements RecordCursor
    {
        private final List<ColumnType> types;
        private final List<?> record;

        private InMemoryRecordCursor(List<ColumnType> types, List<?> record)
        {
            this.types = checkNotNull(ImmutableList.copyOf(types), "types is null");
            this.record = checkNotNull(ImmutableList.copyOf(record), "record is null");
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
        public boolean advanceNextPosition()
        {
            return true;
        }

        @Override
        public ColumnType getType(int field)
        {
            return types.get(field);
        }

        @Override
        public boolean getBoolean(int field)
        {
            checkNotNull(record.get(field), "value is null");
            return (Boolean) record.get(field);
        }

        @Override
        public long getLong(int field)
        {
            checkNotNull(record.get(field), "value is null");
            return (Long) record.get(field);
        }

        @Override
        public double getDouble(int field)
        {
            checkNotNull(record.get(field), "value is null");
            return (Double) record.get(field);
        }

        @Override
        public byte[] getString(int field)
        {
            Object value = record.get(field);
            checkNotNull(value, "value is null");
            if (value instanceof byte[]) {
                return (byte[]) value;
            }
            if (value instanceof String) {
                return ((String) value).getBytes(Charsets.UTF_8);
            }
            throw new IllegalArgumentException("Field " + field + " is not a String, but is a " + value.getClass().getName());
        }

        @Override
        public boolean isNull(int field)
        {
            return record.get(field) == null;
        }

        @Override
        public void close()
        {
        }
    }
}
