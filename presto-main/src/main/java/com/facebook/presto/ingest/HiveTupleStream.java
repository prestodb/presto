package com.facebook.presto.ingest;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.hive.Record;
import com.facebook.presto.hive.RecordIterator;
import com.facebook.presto.hive.SchemaField;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.google.common.base.Charsets;

import java.util.List;

public class HiveTupleStream
        extends AbstractExternalTupleStream
{
    private final RecordIterator recordIterator;
    private final List<SchemaField> schemaFields;
    private Record record;

    public HiveTupleStream(RecordIterator recordIterator, List<SchemaField> schemaFields, TupleInfo tupleInfo)
    {
        super(tupleInfo);
        this.recordIterator = recordIterator;
        this.schemaFields = schemaFields;
    }

    @Override
    protected boolean computeNext()
    {
        if (!recordIterator.hasNext()) {
            return false;
        }
        record = recordIterator.next();
        return true;
    }

    @Override
    protected void buildTuple(TupleInfo.Builder builder)
    {
        int index = 0;
        for (TupleInfo.Type type : tupleInfo.getTypes()) {
            switch (type) {
                case FIXED_INT_64:
                    builder.append(getLong(index));
                    break;
                case DOUBLE:
                    builder.append(getDouble(index));
                    break;
                case VARIABLE_BINARY:
                    builder.append(getSlice(index));
                    break;
                default:
                    throw new AssertionError("unhandled type: " + type);
            }
            index++;
        }
    }

    @Override
    protected long getLong(int field)
    {
        return record.getLong(schemaFields.get(field).getFieldName());
    }

    @Override
    protected double getDouble(int field)
    {
        return record.getDouble(schemaFields.get(field).getFieldName());
    }

    @Override
    protected Slice getSlice(int field)
    {
        String s = record.getString(schemaFields.get(field).getFieldName());
        return Slices.wrappedBuffer(s.getBytes(Charsets.UTF_8));
    }
}
