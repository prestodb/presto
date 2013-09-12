package com.facebook.presto.operator;

import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.operator.RowPageBuilder.rowPageBuilder;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class RowPagesBuilder
{
    public static RowPagesBuilder rowPagesBuilder(TupleInfo... tupleInfos)
    {
        return rowPagesBuilder(ImmutableList.copyOf(tupleInfos));
    }

    public static RowPagesBuilder rowPagesBuilder(Iterable<TupleInfo> tupleInfos)
    {
        return new RowPagesBuilder(tupleInfos);
    }

    private final ImmutableList.Builder<Page> pages = ImmutableList.builder();
    private final List<TupleInfo> tupleInfos;
    private RowPageBuilder builder;

    RowPagesBuilder(Iterable<TupleInfo> tupleInfos)
    {
        this.tupleInfos = ImmutableList.copyOf(checkNotNull(tupleInfos, "tupleInfos is null"));
        builder = rowPageBuilder(tupleInfos);
    }

    public RowPagesBuilder addSequencePage(int length, int... initialValues)
    {
        checkArgument(length > 0, "length must be at least 1");
        checkNotNull(initialValues, "initialValues is null");
        checkArgument(initialValues.length == tupleInfos.size(), "Expected %s initialValues, but got %s", tupleInfos.size(), initialValues.length);

        pageBreak();
        Page page = SequencePageBuilder.createSequencePage(tupleInfos, length, initialValues);
        pages.add(page);
        return this;
    }

    public RowPagesBuilder row(Object... values)
    {
        builder.row(values);
        return this;
    }

    public RowPagesBuilder pageBreak()
    {
        if (!builder.isEmpty()) {
            pages.add(builder.build());
            builder = rowPageBuilder(tupleInfos);
        }
        return this;
    }

    public List<Page> build()
    {
        pageBreak();
        return pages.build();
    }
}
