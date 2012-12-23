/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.operator.AbstractPageIterator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.util.List;

public class RecordProjectOperator
        implements Operator
{
    private final RecordSet source;
    private final DataSize dataSize;
    private final List<TupleInfo> tupleInfos;

    public RecordProjectOperator(RecordSet source, DataSize dataSize, Type... types)
    {
        this(source, dataSize, ImmutableList.copyOf(types));
    }

    public RecordProjectOperator(RecordSet source, DataSize dataSize, Iterable<Type> types)
    {
        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkNotNull(dataSize, "dataSize is null");
        Preconditions.checkNotNull(types, "projections is null");

        this.source = source;
        this.dataSize = dataSize;

        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (Type type : types) {
            tupleInfos.add(new TupleInfo(type));
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
        operatorStats.addActualDataSize(dataSize.toBytes());
        return new RecordProjectionOperator(source.cursor(operatorStats), tupleInfos, operatorStats);
    }

    private static class RecordProjectionOperator
            extends AbstractPageIterator
    {
        private final RecordCursor cursor;
        private final OperatorStats operatorStats;


        public RecordProjectionOperator(RecordCursor cursor, List<TupleInfo> tupleInfos, OperatorStats operatorStats)
        {
            super(tupleInfos);
            this.cursor = cursor;
            this.operatorStats = operatorStats;
        }

        protected Page computeNext()
        {
            // todo convert this code to page builder
            BlockBuilder[] outputs = new BlockBuilder[getChannelCount()];
            for (int i = 0; i < outputs.length; i++) {
                outputs[i] = new BlockBuilder(getTupleInfos().get(i));
            }

            while (!isFull(outputs) && cursor.advanceNextPosition()) {
                for (int field = 0; field < outputs.length; field++) {
                    BlockBuilder output = outputs[field];
                    switch (getTupleInfos().get(field).getTypes().get(0)) {
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

            if (outputs[0].isEmpty()) {
                return endOfData();
            }

            Block[] blocks = new Block[getChannelCount()];
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = outputs[i].build();
            }

            Page page = new Page(blocks);
            operatorStats.addActualPositionCount(page.getPositionCount());
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
