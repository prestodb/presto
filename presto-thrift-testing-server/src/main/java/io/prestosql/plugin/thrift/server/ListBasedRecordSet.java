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
package io.prestosql.plugin.thrift.server;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ListBasedRecordSet
        implements RecordSet
{
    private final List<Type> types;
    private final List<List<String>> keys;
    private final int totalRecords;

    public ListBasedRecordSet(List<List<String>> keys, List<Type> types)
    {
        this.types = requireNonNull(types, "types is null");
        this.keys = requireNonNull(keys, "types is null");
        checkArgument(keys.isEmpty() || keys.size() == types.size(),
                "number of types and key columns must match");
        this.totalRecords = keys.isEmpty() ? 0 : keys.get(0).size();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return types;
    }

    @Override
    public RecordCursor cursor()
    {
        return new ListBasedRecordCursor();
    }

    private class ListBasedRecordCursor
            implements RecordCursor
    {
        private int position = -1;

        @Override
        public boolean isNull(int field)
        {
            return keys.get(field).get(position) == null;
        }

        @Override
        public long getLong(int field)
        {
            return Long.parseLong(keys.get(field).get(position));
        }

        @Override
        public Slice getSlice(int field)
        {
            return Slices.utf8Slice(keys.get(field).get(position));
        }

        @Override
        public boolean advanceNextPosition()
        {
            if (position >= totalRecords) {
                return false;
            }
            position++;
            return position < totalRecords;
        }

        @Override
        public Type getType(int field)
        {
            return types.get(field);
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
        public void close()
        {
        }

        @Override
        public boolean getBoolean(int field)
        {
            throw new UnsupportedOperationException("invalid type");
        }

        @Override
        public double getDouble(int field)
        {
            throw new UnsupportedOperationException("invalid type");
        }

        @Override
        public Object getObject(int field)
        {
            throw new UnsupportedOperationException("invalid type");
        }
    }
}
