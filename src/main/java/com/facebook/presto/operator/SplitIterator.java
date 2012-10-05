/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.AbstractBlockIterator;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.block.BlockIterator;
import com.facebook.presto.block.Cursor.AdvanceResult;
import com.facebook.presto.block.TupleStream;
import com.google.common.collect.ImmutableList;

import java.util.ArrayDeque;
import java.util.List;

import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.Cursor.AdvanceResult.MUST_YIELD;
import static com.facebook.presto.block.Cursor.AdvanceResult.SUCCESS;

public class SplitIterator<T extends TupleStream>
{
    private final TupleInfo tupleInfo;
    private final int maxBufferSize;
    private final BlockIterator<T> source;
    private final List<ArrayDeque<T>> queues;
    private final List<Split> splits;

    public SplitIterator(TupleInfo tupleInfo, int splitCount, int maxBufferSize, BlockIterable<T> source)
    {
        this.tupleInfo = tupleInfo;
        this.maxBufferSize = maxBufferSize;
        this.source = source.iterator();

        ImmutableList.Builder<ArrayDeque<T>> queueBuilder = ImmutableList.builder();
        ImmutableList.Builder<Split> splitBuilder = ImmutableList.builder();
        for (int i = 0; i < splitCount; i++) {
            ArrayDeque<T> queue = new ArrayDeque<>(maxBufferSize);
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

    public BlockIterator<T> getSplit(int index)
    {
        return splits.get(index);
    }

    private AdvanceResult bufferNewBlock()
    {
        for (ArrayDeque<T> queue : queues) {
            if (queue.size() >= maxBufferSize) {
                return MUST_YIELD;
            }
        }

        if (source.mustYield()) {
            return MUST_YIELD;
        }

        if (!source.hasNext()) {
            return FINISHED;
        }

        T next = source.next();
        for (ArrayDeque<T> queue : queues) {
            queue.addLast(next);
        }
        return SUCCESS;
    }

    public class Split extends AbstractBlockIterator<T>
    {
        private final ArrayDeque<T> queue;

        private Split(ArrayDeque<T> queue)
        {
            this.queue = queue;
        }

        @Override
        protected T computeNext()
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
