package com.facebook.presto.spi;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class InMemoryRecordSet
        implements RecordSet
{
    private final List<ColumnType> types;
    private final Iterable<? extends List<?>> records;

    public InMemoryRecordSet(Collection<ColumnType> types, Collection<? extends List<?>> records)
    {
        this.types = Collections.unmodifiableList(new ArrayList<>(types));
        this.records = records;
    }

    @Override
    public List<ColumnType> getColumnTypes()
    {
        return types;
    }

    @Override
    public RecordCursor cursor()
    {
        return new InMemoryRecordCursor(records.iterator());
    }

    private static class InMemoryRecordCursor
            implements RecordCursor
    {
        private final Iterator<? extends List<?>> records;
        private List<?> record;

        private InMemoryRecordCursor(Iterator<? extends List<?>> records)
        {
            this.records = records;
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
            if (!records.hasNext()) {
                record = null;
                return false;
            }
            record = records.next();
            return true;
        }

        @Override
        public boolean getBoolean(int field)
        {
            checkState(record != null, "no current record");
            checkNotNull(record.get(field), "value is null");
            return (Boolean) record.get(field);
        }

        @Override
        public long getLong(int field)
        {
            checkState(record != null, "no current record");
            checkNotNull(record.get(field), "value is null");
            return (Long) record.get(field);
        }

        @Override
        public double getDouble(int field)
        {
            checkState(record != null, "no current record");
            checkNotNull(record.get(field), "value is null");
            return (Double) record.get(field);
        }

        @Override
        public byte[] getString(int field)
        {
            checkState(record != null, "no current record");
            Object value = record.get(field);
            checkNotNull(value, "value is null");
            if (value instanceof byte[]) {
                return (byte[]) value;
            }
            if (value instanceof String) {
                return ((String) value).getBytes(StandardCharsets.UTF_8);
            }
            throw new IllegalArgumentException("Field " + field + " is not a String, but is a " + value.getClass().getName());
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

    public static Builder builder(TableMetadata tableMetadata)
    {
        return builder(tableMetadata.getColumns());
    }

    public static Builder builder(List<ColumnMetadata> columns)
    {
        List<ColumnType> columnTypes = new ArrayList<>();
        for (ColumnMetadata column : columns) {
            columnTypes.add(column.getType());
        }
        return builder(columnTypes);
    }

    public static Builder builder(Collection<ColumnType> columnsTypes)
    {
        return new Builder(columnsTypes);
    }

    public static class Builder
    {
        private final List<ColumnType> types;
        private final List<List<Object>> records = new ArrayList<>();

        private Builder(Collection<ColumnType> types)
        {
            checkNotNull(types, "types is null");
            this.types = Collections.unmodifiableList(new ArrayList<>(types));
            checkArgument(!this.types.isEmpty(), "types is empty");
        }

        public Builder addRow(Object... values)
        {
            checkNotNull(values, "values is null");
            checkArgument(values.length == types.size(), "Expected %s values in row, but got %s values", types.size(), values.length);
            for (int i = 0; i < values.length; i++) {
                Object value = values[i];
                if (value == null) {
                    continue;
                }
                switch (types.get(i)) {
                    case BOOLEAN:
                        checkArgument(value instanceof Boolean, "Expected value %d to be an instance of Boolean, but is a %s", i, value.getClass().getSimpleName());
                        break;
                    case LONG:
                        checkArgument(value instanceof Long, "Expected value %d to be an instance of Long, but is a %s", i, value.getClass().getSimpleName());
                        break;
                    case DOUBLE:
                        checkArgument(value instanceof Double, "Expected value %d to be an instance of Double, but is a %s", i, value.getClass().getSimpleName());
                        break;
                    case STRING:
                        checkArgument(value instanceof String, "Expected value %d to be an instance of String, but is a %s", i, value.getClass().getSimpleName());
                        break;
                    default:
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

    private static void checkNotNull(Object value, String message)
    {
        if (value == null) {
            throw new NullPointerException(message);
        }
    }

    private static void checkState(boolean test, String message)
    {
        if (!test) {
            throw new IllegalStateException(message);
        }
    }    
}
