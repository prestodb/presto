/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.AbstractBlockIterator;
import com.facebook.presto.block.BlockIterator;
import com.facebook.presto.block.Cursor.AdvanceResult;
import com.facebook.presto.block.TupleStream;
import com.google.common.collect.ImmutableList;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.Cursor.AdvanceResult.MUST_YIELD;
import static com.facebook.presto.block.Cursor.AdvanceResult.SUCCESS;

public class SplitIterator
{
    private final TupleInfo tupleInfo;
    private final int maxBufferSize;
    private final Iterator<TupleStream> source;
    private final List<ArrayDeque<TupleStream>> queues;
    private final List<Split> splits;

    public SplitIterator(TupleInfo tupleInfo, int splitCount, int maxBufferSize, Iterable<TupleStream> source)
    {
        this.tupleInfo = tupleInfo;
        this.maxBufferSize = maxBufferSize;
        this.source = source.iterator();

        ImmutableList.Builder<ArrayDeque<TupleStream>> queueBuilder = ImmutableList.builder();
        ImmutableList.Builder<Split> splitBuilder = ImmutableList.builder();
        for (int i = 0; i < splitCount; i++) {
            ArrayDeque<TupleStream> queue = new ArrayDeque<>(maxBufferSize);
            queueBuilder.add(queue);
            splitBuilder.add(new Split(queue));
        }
        queues = queueBuilder.build();
        splits = splitBuilder.build();
    }

    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    public BlockIterator<TupleStream> getSplit(int index)
    {
        return splits.get(index);
    }

    private AdvanceResult bufferNewBlock()
    {
        for (ArrayDeque<TupleStream> queue : queues) {
            if (queue.size() >= maxBufferSize) {
                return MUST_YIELD;
            }
        }

        if (!source.hasNext()) {
            return FINISHED;
        }

        TupleStream next = source.next();
        for (ArrayDeque<TupleStream> queue : queues) {
            queue.addLast(next);
        }
        return SUCCESS;
    }

    public class Split extends AbstractBlockIterator<TupleStream>
    {
        private final ArrayDeque<TupleStream> queue;

        private Split(ArrayDeque<TupleStream> queue)
        {
            this.queue = queue;
        }

        @Override
        protected TupleStream computeNext()
        {
            // if the queue is empty, attempt to buffer a new block
            if (queue.isEmpty()) {
                switch (bufferNewBlock()) {
                    case MUST_YIELD:
                        return setMustYield();
                    case FINISHED:
                        return endOfData();
                }
            }
            return queue.removeFirst();
        }
    }
}
