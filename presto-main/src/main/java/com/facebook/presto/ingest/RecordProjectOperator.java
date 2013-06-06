/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.operator.AbstractPageIterator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class RecordProjectOperator
        implements Operator
{
    private final RecordSet source;
    private final List<TupleInfo> tupleInfos;

    public RecordProjectOperator(RecordSet source)
    {
        Preconditions.checkNotNull(source, "source is null");

        this.source = source;

        // project each field into a separate channel
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (ColumnType columnType : source.getColumnTypes()) {
            tupleInfos.add(new TupleInfo(Type.fromColumnType(columnType)));
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
        return new RecordProjectionOperator(source.cursor(), tupleInfos, operatorStats);
    }

    private static class RecordProjectionOperator
            extends AbstractPageIterator
    {
        private final RecordCursor cursor;
        private final OperatorStats operatorStats;
        private long currentCompletedSize;
        private final PageBuilder pageBuilder;

        public RecordProjectionOperator(RecordCursor cursor, List<TupleInfo> tupleInfos, OperatorStats operatorStats)
        {
            super(tupleInfos);

            this.cursor = cursor;
            this.operatorStats = operatorStats;
            operatorStats.addDeclaredSize(cursor.getTotalBytes());
            pageBuilder = new PageBuilder(getTupleInfos());
        }

        protected Page computeNext()
        {
            if (operatorStats.isDone()) {
                return endOfData();
            }

            pageBuilder.reset();
            while (!pageBuilder.isFull() && cursor.advanceNextPosition()) {
                for (int column = 0; column < super.getChannelCount(); column++) {
                    BlockBuilder output = pageBuilder.getBlockBuilder(column);
                    if (cursor.isNull(column)) {
                        output.appendNull();
                    }
                    else {
                        switch (getTupleInfos().get(column).getTypes().get(0)) {
                            case BOOLEAN:
                                output.append(cursor.getBoolean(column));
                                break;
                            case FIXED_INT_64:
                                output.append(cursor.getLong(column));
                                break;
                            case DOUBLE:
                                output.append(cursor.getDouble(column));
                                break;
                            case VARIABLE_BINARY:
                                output.append(cursor.getString(column));
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

            if (pageBuilder.isEmpty()) {
                return endOfData();
            }

            Page page = pageBuilder.build();
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
