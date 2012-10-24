/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.AbstractYieldingIterator;
import com.facebook.presto.block.YieldingIterable;
import com.facebook.presto.block.YieldingIterator;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursor.AdvanceResult;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayDeque;
import java.util.List;

import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.Cursor.AdvanceResult.MUST_YIELD;
import static com.facebook.presto.block.Cursor.AdvanceResult.SUCCESS;

public class Splitter<T extends TupleStream>
{
    private final TupleInfo tupleInfo;
    private final int splitCount;
    private final int maxBufferSize;
    private final YieldingIterable<T> source;

    public Splitter(TupleInfo tupleInfo, int splitCount, int maxBufferSize, YieldingIterable<T> source)
    {
        this.tupleInfo = tupleInfo;
        this.splitCount = splitCount;
        this.maxBufferSize = maxBufferSize;
        this.source = source;
    }

    public SplitTupleStream getSplit(int index)
    {
        Preconditions.checkPositionIndex(index, splitCount);
        return new SplitTupleStream(index);
    }

    private SplitterState<T> getSplitterState(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        SplitterState<T> splitterState = (SplitterState<T>) session.getData(this);
        if (splitterState == null) {
            splitterState = new SplitterState<>(tupleInfo, splitCount, maxBufferSize, source);
            session.putData(this, splitterState);
        }
        return splitterState;
    }

    public class SplitTupleStream implements TupleStream, YieldingIterable<T>
    {
        private final int index;

        public SplitTupleStream(int index)
        {
            this.index = index;
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return tupleInfo;
        }

        @Override
        public Range getRange()
        {
            return Range.ALL;
        }

        @Override
        public Cursor cursor(QuerySession session)
        {
            Preconditions.checkNotNull(session, "session is null");
            return new GenericCursor(session, tupleInfo, iterator(session));
        }

        @Override
        public YieldingIterator<T> iterator(QuerySession session)
        {
            Preconditions.checkNotNull(session, "session is null");
            return getSplitterState(session).getSplit(index);
        }
    }

    private static final class SplitterState<T extends TupleStream>
    {
        private final TupleInfo tupleInfo;
        private final int maxBufferSize;
        private final YieldingIterator<T> source;
        private final List<ArrayDeque<T>> queues;
        private final List<SplitYieldingIterator<T>> splits;

        public SplitterState(TupleInfo tupleInfo, int splitCount, int maxBufferSize, YieldingIterable<T> source)
        {
            this.tupleInfo = tupleInfo;
            this.maxBufferSize = maxBufferSize;
            this.source = source.iterator(new QuerySession());// todo this is not correct

            ImmutableList.Builder<ArrayDeque<T>> queueBuilder = ImmutableList.builder();
            ImmutableList.Builder<SplitYieldingIterator<T>> splitBuilder = ImmutableList.builder();
            for (int i = 0; i < splitCount; i++) {
                ArrayDeque<T> queue = new ArrayDeque<>(maxBufferSize);
                queueBuilder.add(queue);
                splitBuilder.add(new SplitYieldingIterator<>(this, queue));
            }
            queues = queueBuilder.build();
            splits = splitBuilder.build();
        }

        public TupleInfo getTupleInfo()
        {
            return tupleInfo;
        }

        public SplitYieldingIterator<T> getSplit(int index)
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
    }

    private static class SplitYieldingIterator<T extends TupleStream> extends AbstractYieldingIterator<T>
    {
        private final SplitterState<T> state;
        private final ArrayDeque<T> queue;

        private SplitYieldingIterator(SplitterState<T> state, ArrayDeque<T> queue)
        {
            this.state = state;
            this.queue = queue;
        }

        @Override
        protected T computeNext()
        {
            // if the queue is empty, attempt to buffer a new block
            if (queue.isEmpty()) {
                switch (state.bufferNewBlock()) {
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
