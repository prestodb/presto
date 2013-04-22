package com.facebook.presto.metadata;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.block.BlockIterables.createBlockIterable;
import static com.facebook.presto.block.BlockUtils.emptyBlockIterable;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class InternalTable
{
    private final List<BlockIterable> columns;

    public InternalTable(List<BlockIterable> columns)
    {
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
    }

    public BlockIterable getColumn(int index)
    {
        return columns.get(index);
    }

    public static Builder builder(TupleInfo tupleInfo)
    {
        return new Builder(tupleInfo);
    }

    public static Builder builder(List<ColumnMetadata> columns)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ColumnMetadata column : columns) {
            types.add(column.getType());
        }
        return new Builder(new TupleInfo(types.build()));
    }

    public static class Builder
    {
        private final TupleInfo tupleInfo;
        private final List<TupleInfo> tupleInfos;
        private final List<List<Block>> columns;
        private PageBuilder pageBuilder;

        public Builder(TupleInfo tupleInfo)
        {
            this.tupleInfo = checkNotNull(tupleInfo, "tupleInfo is null");
            tupleInfos = getTupleInfos(tupleInfo);

            columns = new ArrayList<>();
            for (int i = 0; i < tupleInfo.getFieldCount(); i++) {
                columns.add(new ArrayList<Block>());
            }

            pageBuilder = new PageBuilder(tupleInfos);
        }

        public TupleInfo getTupleInfo()
        {
            return tupleInfo;
        }

        public Builder add(Tuple tuple)
        {
            checkArgument(tuple.getTupleInfo().equals(tupleInfo), "tuple schema does not match builder");

            for (int i = 0; i < tupleInfo.getFieldCount(); i++) {
                pageBuilder.getBlockBuilder(i).append(tuple, i);
            }

            if (pageBuilder.isFull()) {
                flushPage();
                pageBuilder = new PageBuilder(tupleInfos);
            }
            return this;
        }

        public InternalTable build()
        {
            flushPage();
            ImmutableList.Builder<BlockIterable> list = ImmutableList.builder();
            for (List<Block> column : columns) {
                list.add(column.isEmpty() ? emptyBlockIterable() : createBlockIterable(column.get(0).getTupleInfo(), column));
            }
            return new InternalTable(list.build());
        }

        private void flushPage()
        {
            if (!pageBuilder.isEmpty()) {
                Page page = pageBuilder.build();
                for (int i = 0; i < tupleInfo.getFieldCount(); i++) {
                    columns.get(i).add(page.getBlock(i));
                }
            }
        }

        private static List<TupleInfo> getTupleInfos(TupleInfo tupleInfo)
        {
            ImmutableList.Builder<TupleInfo> list = ImmutableList.builder();
            for (TupleInfo.Type type : tupleInfo.getTypes()) {
                list.add(new TupleInfo(type));
            }
            return list.build();
        }
    }
}
