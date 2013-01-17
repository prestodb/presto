/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.metadata.ImportColumnHandle;
import com.facebook.presto.operator.AbstractPageIterator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;

import static com.facebook.presto.metadata.ImportColumnHandle.idGetter;

public class RecordProjectOperator
        implements Operator
{
    private final RecordSet source;
    private final List<TupleInfo> tupleInfos;
    private final List<Integer> columnIds;

    public RecordProjectOperator(RecordSet source, ImportColumnHandle... columns)
    {
        this(source, ImmutableList.copyOf(columns));
    }

    public RecordProjectOperator(RecordSet source, Iterable<ImportColumnHandle> columns)
    {
        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkNotNull(columns, "columns is null");

        this.source = source;

        this.columnIds = ImmutableList.copyOf(Iterables.transform(columns, idGetter()));

        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (ImportColumnHandle column : columns) {
            tupleInfos.add(new TupleInfo(column.getColumnType()));
        }
        this.tupleInfos = tupleInfos.build();
    }

    @Override
    public int getChannelCount()
    {
        return tupleInfos.size();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public PageIterator iterator(OperatorStats operatorStats)
    {
        return new RecordProjectionOperator(source.cursor(operatorStats), tupleInfos, columnIds, operatorStats);
    }

    private static class RecordProjectionOperator
            extends AbstractPageIterator
    {
        private final RecordCursor cursor;
        private final List<Integer> columnIds;
        private final OperatorStats operatorStats;
        private long currentCompletedSize;

        public RecordProjectionOperator(RecordCursor cursor, List<TupleInfo> tupleInfos, List<Integer> columnIds, OperatorStats operatorStats)
        {
            super(tupleInfos);

            this.cursor = cursor;
            this.columnIds = columnIds;
            this.operatorStats = operatorStats;
            operatorStats.addDeclaredSize(cursor.getTotalBytes());
        }

        protected Page computeNext()
        {
            if (operatorStats.isDone()) {
                return endOfData();
            }

            // todo convert this code to page builder
            BlockBuilder[] outputs = new BlockBuilder[getChannelCount()];
            for (int i = 0; i < outputs.length; i++) {
                outputs[i] = new BlockBuilder(getTupleInfos().get(i));
            }

            while (!isFull(outputs) && cursor.advanceNextPosition()) {
                for (int i = 0; i < columnIds.size(); i++) {
                    int field = columnIds.get(i);
                    BlockBuilder output = outputs[i];
                    if (cursor.isNull(field)) {
                        output.appendNull();
                    } else {
                        switch (getTupleInfos().get(i).getTypes().get(0)) {
                            case FIXED_INT_64:
                                output.append(cursor.getLong(field));
                                break;
                            case DOUBLE:
                                output.append(cursor.getDouble(field));
                                break;
                            case VARIABLE_BINARY:
                                output.append(cursor.getString(field));
                                break;
                        }
                    }
                }
            }

            // update completed size after each page is produced
            long completedDataSize = cursor.getCompletedBytes();
            if (completedDataSize > currentCompletedSize) {
                operatorStats.addCompletedDataSize(completedDataSize - currentCompletedSize);
                currentCompletedSize = completedDataSize;
            }

            if (outputs[0].isEmpty()) {
                return endOfData();
            }

            Block[] blocks = new Block[getChannelCount()];
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = outputs[i].build();
            }

            Page page = new Page(blocks);
            operatorStats.addCompletedPositions(page.getPositionCount());
            return page;
        }

        @Override
        protected void doClose()
        {
            cursor.close();
        }

        private boolean isFull(BlockBuilder... outputs)
        {
            for (BlockBuilder output : outputs) {
                if (output.isFull()) {
                    return true;
                }
            }
            return false;
        }
    }
}
