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
package com.facebook.presto.spi;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class InMemoryRecordSet
        implements RecordSet
{
    private final List<Type> types;
    private final Iterable<? extends List<?>> records;

    public InMemoryRecordSet(Collection<? extends Type> types, Iterable<? extends List<?>> records)
    {
        this.types = Collections.unmodifiableList(new ArrayList<>(types));
        this.records = records;
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return types;
    }

    @Override
    public RecordCursor cursor()
    {
        return new InMemoryRecordCursor(types, records.iterator());
    }

    private static class InMemoryRecordCursor
            implements RecordCursor
    {
        private final List<Type> types;
        private final Iterator<? extends List<?>> records;
        private List<?> record;
        private long completedBytes;

        private InMemoryRecordCursor(List<Type> types, Iterator<? extends List<?>> records)
        {
            this.types = types;
            this.records = records;
        }

        @Override
        public long getCompletedBytes()
        {
            return completedBytes;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            return types.get(field);
        }

        @Override
        public boolean advanceNextPosition()
        {
            if (!records.hasNext()) {
                record = null;
                return false;
            }
            record = records.next();
            completedBytes += sizeOf(record);

            return true;
        }

        @Override
        public boolean getBoolean(int field)
        {
            checkState(record != null, "no current record");
            requireNonNull(record.get(field), "value is null");
            return (Boolean) record.get(field);
        }

        @Override
        public long getLong(int field)
        {
            checkState(record != null, "no current record");
            requireNonNull(record.get(field), "value is null");
            return ((Number) record.get(field)).longValue();
        }

        @Override
        public double getDouble(int field)
        {
            checkState(record != null, "no current record");
            requireNonNull(record.get(field), "value is null");
            return (Double) record.get(field);
        }

        @Override
        public Slice getSlice(int field)
        {
            checkState(record != null, "no current record");
            Object value = record.get(field);
            requireNonNull(value, "value is null");
            if (value instanceof byte[]) {
                return Slices.wrappedBuffer((byte[]) value);
            }
            if (value instanceof String) {
                return Slices.utf8Slice((String) value);
            }
            if (value instanceof Slice) {
                return (Slice) value;
            }
            throw new IllegalArgumentException("Field " + field + " is not a String, but is a " + value.getClass().getName());
        }

        @Override
        public Object getObject(int field)
        {
            checkState(record != null, "no current record");
            Object value = record.get(field);
            requireNonNull(value, "value is null");
            return value;
        }

        @Override
        public boolean isNull(int field)
        {
            checkState(record != null, "no current record");
            return record.get(field) == null;
        }

        @Override
        public void close()
        {
        }
    }

    public static Builder builder(ConnectorTableMetadata tableMetadata)
    {
        return builder(tableMetadata.getColumns());
    }

    public static Builder builder(List<ColumnMetadata> columns)
    {
        List<Type> columnTypes = new ArrayList<>();
        for (ColumnMetadata column : columns) {
            columnTypes.add(column.getType());
        }
        return builder(columnTypes);
    }

    public static Builder builder(Collection<Type> columnsTypes)
    {
        return new Builder(columnsTypes);
    }

    public static class Builder
    {
        private final List<Type> types;
        private final List<List<Object>> records = new ArrayList<>();

        private Builder(Collection<Type> types)
        {
            requireNonNull(types, "types is null");
            this.types = Collections.unmodifiableList(new ArrayList<>(types));
            checkArgument(!this.types.isEmpty(), "types is empty");
        }

        public Builder addRow(Object... values)
        {
            requireNonNull(values, "values is null");
            checkArgument(values.length == types.size(), "Expected %s values in row, but got %s values", types.size(), values.length);
            for (int i = 0; i < values.length; i++) {
                Object value = values[i];
                if (value == null) {
                    continue;
                }

                Type type = types.get(i);
                if (BOOLEAN.equals(type)) {
                    checkArgument(value instanceof Boolean, "Expected value %d to be an instance of Boolean, but is a %s", i, value.getClass().getSimpleName());
                }
                else if (INTEGER.equals(type)) {
                    checkArgument(value instanceof Integer, "Expected value %d to be an instance of Integer, but is a %s", i, value.getClass().getSimpleName());
                }
                else if (BIGINT.equals(type) || DATE.equals(type) || TIMESTAMP.equals(type) || TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
                    checkArgument(value instanceof Integer || value instanceof Long,
                            "Expected value %d to be an instance of Integer or Long, but is a %s", i, value.getClass().getSimpleName());
                }
                else if (DOUBLE.equals(type)) {
                    checkArgument(value instanceof Double, "Expected value %d to be an instance of Double, but is a %s", i, value.getClass().getSimpleName());
                }
                else if (VARCHAR.equals(type)) {
                    checkArgument(value instanceof String || value instanceof byte[],
                            "Expected value %d to be an instance of String or byte[], but is a %s", i, value.getClass().getSimpleName());
                }
                else if (VARBINARY.equals(type)) {
                    checkArgument(value instanceof Slice,
                            "Expected value %d to be an instance of Slice, but is a %s", i, value.getClass().getSimpleName());
                }
                else if (type.getTypeSignature().getBase().equals("array")) {
                    checkArgument(value instanceof Block,
                            "Expected value %d to be an instance of Block, but is a %s", i, value.getClass().getSimpleName());
                }
                else if (type.getTypeSignature().getBase().equals("row")) {
                    checkArgument(value instanceof Block,
                            "Expected value %d to be an instance of Block, but is a %s", i, value.getClass().getSimpleName());
                }
                else {
                    throw new IllegalStateException("Unsupported column type " + types.get(i));
                }
            }
            // Immutable list does not allow nulls
            records.add(Collections.unmodifiableList(new ArrayList<>(Arrays.asList(values))));
            return this;
        }

        public InMemoryRecordSet build()
        {
            return new InMemoryRecordSet(types, records);
        }
    }

    private static void checkArgument(boolean test, String message, Object... args)
    {
        if (!test) {
            throw new IllegalArgumentException(String.format(message, args));
        }
    }

    private static void checkState(boolean test, String message)
    {
        if (!test) {
            throw new IllegalStateException(message);
        }
    }

    private static long sizeOf(List<?> record)
    {
        long completedBytes = 0;
        for (Object value : record) {
            if (value == null) {
                // do nothing
            }
            else if (value instanceof Boolean) {
                completedBytes++;
            }
            else if (value instanceof Number) {
                completedBytes += 8;
            }
            else if (value instanceof String) {
                completedBytes += ((String) value).length();
            }
            else if (value instanceof byte[]) {
                completedBytes += ((byte[]) value).length;
            }
            else if (value instanceof Block) {
                completedBytes += ((Block) value).getSizeInBytes();
            }
            else if (value instanceof Slice) {
                completedBytes += ((Slice) value).length();
            }
            else {
                throw new IllegalArgumentException("Unknown type: " + value.getClass());
            }
        }
        return completedBytes;
    }
}
