package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.TableMetadata;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.metadata.MetadataUtil.columnTypeGetter;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

public class InMemoryRecordSet
        implements RecordSet
{
    private final List<ColumnType> types;
    private final Iterable<? extends List<?>> records;

    public InMemoryRecordSet(Iterable<ColumnType> types, Iterable<? extends List<?>> records)
    {
        this.types = ImmutableList.copyOf(types);
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
        public long getLong(int field)
        {
            Preconditions.checkState(record != null, "no current record");
            checkNotNull(record.get(field), "value is null");
            return (Long) record.get(field);
        }

        @Override
        public double getDouble(int field)
        {
            Preconditions.checkState(record != null, "no current record");
            checkNotNull(record.get(field), "value is null");
            return (Double) record.get(field);
        }

        @Override
        public byte[] getString(int field)
        {
            Preconditions.checkState(record != null, "no current record");
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
            Preconditions.checkState(record != null, "no current record");
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
        return builder(transform(columns, columnTypeGetter()));
    }

    public static Builder builder(Iterable<ColumnType> columnsTypes)
    {
        return new Builder(columnsTypes);
    }

    public static class Builder
    {
        private final List<ColumnType> types;
        private final ImmutableList.Builder<? extends List<?>> records = ImmutableList.builder();

        private Builder(Iterable<ColumnType> types)
        {
            checkNotNull(types, "types is null");
            this.types = ImmutableList.copyOf(types);
            checkArgument(!this.types.isEmpty(), "types is empty");
        }

        public Builder addRow(Object... values)
        {
            checkNotNull(values, "values is null");
            checkArgument(values.length == types.size());
            for (int i = 0; i < values.length; i++) {
                Object value = values[i];
                if (value == null) {
                    continue;
                }
                switch (types.get(i)) {
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
            records.add(Collections.<Object>unmodifiableList(new ArrayList<>(Arrays.asList(values))));
            return this;
        }

        public InMemoryRecordSet build()
        {
            return new InMemoryRecordSet(types, records.build());
        }
    }
}
