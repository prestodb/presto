/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.nblock.BlockBuilder;
import com.google.common.base.Charsets;

public final class RecordProjections {
    private RecordProjections()
    {
    }

    public static RecordProjection createProjection(int field, Type type)
    {
        return new SimpleRecordProjection(type, field);
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
            switch (type) {
                case FIXED_INT_64:
                    output.append(record.getLong(field));
                    break;
                case VARIABLE_BINARY:
                    output.append(record.getString(field).getBytes(Charsets.UTF_8));
                    break;
                case DOUBLE:
                    output.append(record.getDouble(field));
                    break;
            }
        }
    }
}
