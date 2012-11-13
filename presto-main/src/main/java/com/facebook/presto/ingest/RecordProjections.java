/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;

import java.util.List;

public final class RecordProjections {
    private RecordProjections()
    {
    }

    public static RecordProjection createProjection(int field, Type type)
    {
        return new SimpleRecordProjection(type, field);
    }

    public static List<RecordProjection> createProjections(Type... types)
    {
        return createProjections(ImmutableList.copyOf(types));
    }

    public static List<RecordProjection> createProjections(Iterable<Type> types)
    {
        ImmutableList.Builder<RecordProjection> list = ImmutableList.builder();
        int field = 0;
        for (Type type : types) {
            list.add(createProjection(field, type));
            field++;
        }
        return list.build();
    }

    private static class SimpleRecordProjection implements RecordProjection
    {
        private final Type type;
        private final int field;

        public SimpleRecordProjection(Type type, int field)
        {
            this.type = type;
            this.field = field;
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return new TupleInfo(type);
        }

        @Override
        public void project(Record record, BlockBuilder output)
        {
            if (record.isNull(field)) {
                output.appendNull();
            } else {
                switch (type) {
                    case FIXED_INT_64: {
                        long value = record.getLong(field);
                        output.append(value);
                        break;
                    }
                    case VARIABLE_BINARY: {
                        String value = record.getString(field);
                        output.append(value.getBytes(Charsets.UTF_8));
                        break;
                    }
                    case DOUBLE: {
                        double value = record.getDouble(field);
                        output.append(value);
                        break;
                    }
                }
            }
        }
    }
}
