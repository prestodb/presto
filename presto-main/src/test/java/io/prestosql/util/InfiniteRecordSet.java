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
package io.prestosql.util;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class InfiniteRecordSet
        implements RecordSet
{
    private final List<Type> types;
    private final List<?> record;

    public InfiniteRecordSet(List<Type> types, List<?> record)
    {
        this.types = types;
        this.record = record;
    }

    @Override
    public List<Type> getColumnTypes()
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
        private final List<Type> types;
        private final List<?> record;

        private InMemoryRecordCursor(List<Type> types, List<?> record)
        {
            this.types = requireNonNull(ImmutableList.copyOf(types), "types is null");
            this.record = requireNonNull(ImmutableList.copyOf(record), "record is null");
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
        public boolean advanceNextPosition()
        {
            return true;
        }

        @Override
        public Type getType(int field)
        {
            return types.get(field);
        }

        @Override
        public boolean getBoolean(int field)
        {
            requireNonNull(record.get(field), "value is null");
            return (Boolean) record.get(field);
        }

        @Override
        public long getLong(int field)
        {
            requireNonNull(record.get(field), "value is null");
            return (Long) record.get(field);
        }

        @Override
        public double getDouble(int field)
        {
            requireNonNull(record.get(field), "value is null");
            return (Double) record.get(field);
        }

        @Override
        public Slice getSlice(int field)
        {
            Object value = record.get(field);
            requireNonNull(value, "value is null");
            if (value instanceof byte[]) {
                return Slices.wrappedBuffer((byte[]) value);
            }
            if (value instanceof String) {
                return Slices.utf8Slice((String) value);
            }
            throw new IllegalArgumentException("Field " + field + " is not a String, but is a " + value.getClass().getName());
        }

        @Override
        public Object getObject(int field)
        {
            Object value = record.get(field);
            requireNonNull(value, "value is null");
            return value;
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
