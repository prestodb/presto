package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class RowPageBuilder
{
    public static RowPageBuilder rowPageBuilder(TupleInfo... tupleInfos)
    {
        return rowPageBuilder(ImmutableList.copyOf(tupleInfos));
    }

    public static RowPageBuilder rowPageBuilder(Iterable<TupleInfo> tupleInfos)
    {
        return new RowPageBuilder(tupleInfos);
    }

    private final List<BlockBuilder> builders;
    private long rowCount;

    RowPageBuilder(Iterable<TupleInfo> tupleInfos)
    {
        checkNotNull(tupleInfos, "tupleInfos is null");
        ImmutableList.Builder<BlockBuilder> builders = ImmutableList.builder();
        for (TupleInfo tupleInfo : tupleInfos) {
            builders.add(new BlockBuilder(tupleInfo));
        }
        this.builders = builders.build();
        checkArgument(!this.builders.isEmpty(), "At least one tuple info is required");
    }

    public boolean isEmpty()
    {
        return rowCount == 0;
    }

    public RowPageBuilder row(Object... values)
    {
        int channel = 0;
        int field = 0;

        for (Object value : values) {
            BlockBuilder builder = builders.get(channel);
            append(builder, value);

            field++;
            if (field >= builder.getTupleInfo().getFieldCount()) {
                channel++;
                field = 0;
            }
        }
        rowCount++;
        return this;
    }

    public Page build()
    {
        Block[] blocks = new Block[builders.size()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = builders.get(i).build();
        }
        return new Page(blocks);
    }

    private void append(BlockBuilder builder, Object value)
    {
        if (value == null) {
            builder.appendNull();
        }
        else if (value instanceof Boolean) {
            builder.append((Boolean) value);
        }
        else if ((value instanceof Long) || (value instanceof Integer)) {
            builder.append(((Number) value).longValue());
        }
        else if (value instanceof Double) {
            builder.append((Double) value);
        }
        else if (value instanceof String) {
            builder.append((String) value);
        }
        else {
            throw new IllegalArgumentException("bad value: " + value.getClass().getName());
        }
    }
}
